// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License
package service

import (
	"context"
	"fmt"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"

	"github.com/selectdb/ccr_syncer/pkg/ccr"
	"github.com/selectdb/ccr_syncer/pkg/storage"
)

// GlobalDatabaseMonitor 是一个全局变量，用于在不同组件之间共享 DatabaseMonitor 实例
var GlobalDatabaseMonitor *DatabaseMonitor

// DatabaseMonitor 管理数据库监控任务
type DatabaseMonitor struct {
	ctx           context.Context
	cancel        context.CancelFunc
	wg            sync.WaitGroup
	db            storage.DB
	jobManager    *ccr.JobManager
	checkInterval time.Duration
	monitor       *databaseMonitorTask // 单个集群级别监控任务
	mu            sync.Mutex
}

// databaseMonitorTask 表示单个数据库监控任务
type databaseMonitorTask struct {
	request           *CreateCcrRequest
	existingDatabases map[string]bool
}

// NewDatabaseMonitor 创建一个新的数据库监控管理器
func NewDatabaseMonitor(ctx context.Context, db storage.DB, jobManager *ccr.JobManager) *DatabaseMonitor {
	monitorCtx, cancel := context.WithCancel(ctx)
	return &DatabaseMonitor{
		ctx:           monitorCtx,
		cancel:        cancel,
		db:            db,
		jobManager:    jobManager,
		checkInterval: 2 * time.Minute, // 默认检查间隔
		monitor:       nil,
	}
}

// SetCheckInterval 设置检查间隔
func (m *DatabaseMonitor) SetCheckInterval(interval time.Duration) {
	m.checkInterval = interval
}

// AddMonitorTask 添加一个数据库监控任务
func (m *DatabaseMonitor) AddMonitorTask(request *CreateCcrRequest, initialDatabases []string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// 初始化数据库跟踪
	existingDatabases := make(map[string]bool)
	for _, dbName := range initialDatabases {
		existingDatabases[dbName] = true

		// 记录数据库任务信息到monitor_db_job表
		jobName := fmt.Sprintf("%s_%s", request.Name, dbName)
		if err := m.db.AddMonitorJob(dbName, jobName, request.Name, true); err != nil {
			log.Warnf("Failed to add monitor job for database %s: %v", dbName, err)
			return fmt.Errorf("failed to add monitor job for database %s: %w", dbName, err)
		}
	}

	// 创建监控任务
	task := &databaseMonitorTask{
		request:           request,
		existingDatabases: existingDatabases,
	}

	// 存储任务
	m.monitor = task

	log.Infof("Added database monitor task for %s with %d initial databases",
		request.Name, len(initialDatabases))

	return nil
}

// RemoveMonitorTask 移除一个数据库监控任务
func (m *DatabaseMonitor) RemoveMonitorTask(taskName string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.monitor != nil && m.monitor.request.Name == taskName {
		m.monitor = nil
		log.Infof("Removed database monitor task for %s", taskName)
	}
}

// RecoverMonitorTask 从数据库中恢复监控任务
func (m *DatabaseMonitor) RecoverMonitorTask() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// 如果已经有监控任务，则不需要恢复
	if m.monitor != nil {
		log.Infof("Monitor task already exists, no need to recover")
		return nil
	}

	// 从数据库中获取所有活跃的监控任务
	jobs, err := m.db.GetMonitorJobs()
	if err != nil {
		log.Warnf("Failed to get monitor jobs: %v", err)
		return fmt.Errorf("failed to get monitor jobs: %w", err)
	}

	if len(jobs) == 0 {
		log.Infof("No monitor jobs found in database, nothing to recover")
		return nil
	}

	// 创建数据库映射
	existingDatabases := make(map[string]bool)

	// 使用第一个任务的 RequestName 作为请求名称
	var requestName string

	// 处理所有监控任务
	for _, job := range jobs {
		if job.IsActive {
			existingDatabases[job.DBName] = true

			// 获取请求名称
			if requestName == "" && job.RequestName != "" {
				requestName = job.RequestName
			}

			log.Infof("Recovered database %s from monitor job %s", job.DBName, job.JobName)
		}
	}

	// 如果没有找到请求名称，使用默认名称
	if requestName == "" {
		requestName = "ccr_"
	}

	// 创建恢复请求
	request := &CreateCcrRequest{
		Name: requestName,
	}

	// 创建监控任务
	m.monitor = &databaseMonitorTask{
		request:           request,
		existingDatabases: existingDatabases,
	}

	log.Infof("Successfully recovered monitor task with %d databases", len(existingDatabases))

	return nil
}

// Start 启动数据库监控
func (m *DatabaseMonitor) Start() {
	m.wg.Add(1)
	go func() {
		defer m.wg.Done()
		ticker := time.NewTicker(m.checkInterval)
		defer ticker.Stop()

		for {
			select {
			case <-m.ctx.Done():
				log.Infof("Database monitor is shutting down")
				return
			case <-ticker.C:
				// 由于只有一个 cluster 级别的任务，直接检查单个任务
				m.checkClusterTask()
			}
		}
	}()
	log.Infof("Database monitor started with check interval %v", m.checkInterval)
}

// Stop 停止数据库监控
func (m *DatabaseMonitor) Stop() {
	log.Infof("Stopping database monitor...")
	m.cancel()
	m.wg.Wait()
	log.Infof("Database monitor stopped")
}

// checkClusterTask 检查集群监控任务
func (m *DatabaseMonitor) checkClusterTask() {
	m.mu.Lock()
	// 检查是否有监控任务
	if m.monitor == nil {
		m.mu.Unlock()
		log.Debugf("No cluster task to monitor")
		return
	}

	// 获取任务
	task := m.monitor
	m.mu.Unlock()

	// 检查任务
	m.monitorDatabaseChanges(task)
}

// monitorDatabaseChanges 监控数据库变化
func (m *DatabaseMonitor) monitorDatabaseChanges(task *databaseMonitorTask) {
	currentDatabases, err := task.request.Src.GetAllDatabases()
	if err != nil {
		log.Errorf("Failed to get database list for task %s: %v", task.request.Name, err)
		return
	}

	currentDatabaseMap := make(map[string]bool)
	for _, dbName := range currentDatabases {
		if dbName == "" {
			continue
		}
		currentDatabaseMap[dbName] = true
	}

	newDatabases := m.identifyNewDatabases(currentDatabases, task.existingDatabases)
	deletedDatabases := m.identifyDeletedDatabases(task.existingDatabases, currentDatabaseMap)

	m.handleNewDatabases(newDatabases, task.request)
	m.handleDeletedDatabases(deletedDatabases, task.request)
	m.logMonitoringStatus(newDatabases, deletedDatabases, task.existingDatabases, task.request.Name)

	// 记录更新后的数据库状态
	if len(newDatabases) > 0 || len(deletedDatabases) > 0 {
		var existingDBs []string
		for dbName := range task.existingDatabases {
			existingDBs = append(existingDBs, dbName)
		}
		log.Infof("Updated database state for task %s: current databases are %v",
			task.request.Name, existingDBs)
	}
}

// identifyNewDatabases 识别新数据库
func (m *DatabaseMonitor) identifyNewDatabases(currentDatabases []string, existingDatabases map[string]bool) []string {
	var newDatabases []string
	for _, dbName := range currentDatabases {
		if !existingDatabases[dbName] {
			newDatabases = append(newDatabases, dbName)
			existingDatabases[dbName] = true
		}
	}
	return newDatabases
}

// identifyDeletedDatabases 识别已删除的数据库
func (m *DatabaseMonitor) identifyDeletedDatabases(existingDatabases map[string]bool, currentDatabaseMap map[string]bool) []string {
	var deletedDatabases []string
	for dbName := range existingDatabases {
		if !currentDatabaseMap[dbName] {
			deletedDatabases = append(deletedDatabases, dbName)
			delete(existingDatabases, dbName)
		}
	}
	return deletedDatabases
}

// handleNewDatabases 处理新数据库
func (m *DatabaseMonitor) handleNewDatabases(newDatabases []string, request *CreateCcrRequest) {
	if len(newDatabases) == 0 {
		return
	}

	log.Infof("Found %d new databases for task %s: %v", len(newDatabases), request.Name, newDatabases)

	for _, dbName := range newDatabases {
		if dbName == "" {
			log.Warnf("Skipping empty database name")
			continue
		}

		jobName := fmt.Sprintf("%s_%s", request.Name, dbName)
		jobExists, err := m.db.IsJobExist(jobName)
		if err != nil {
			log.Warnf("Error checking if job %s exists: %v", jobName, err)
			continue
		}

		if jobExists {
			log.Warnf("Job %s already exists, skipping sync task creation for database %s", jobName, dbName)
			continue
		}

		dbRequest := &CreateCcrRequest{
			Name:             jobName,
			Src:              request.Src,
			Dest:             request.Dest,
			SkipError:        request.SkipError,
			AllowTableExists: request.AllowTableExists,
			ReuseBinlogLabel: request.ReuseBinlogLabel,
			ClusterSync:      false, // Set to false to avoid recursive calls
		}

		dbRequest.Src.Database = dbName
		dbRequest.Dest.Database = dbName

		maxRetries := 3
		for i := 0; i < maxRetries; i++ {
			if err := createCcr(dbRequest, m.db, m.jobManager); err != nil {
				if i == maxRetries-1 {
					log.Warnf("Failed to create sync task for new database %s (attempt %d/%d): %v",
						dbName, i+1, maxRetries, err)
				} else {
					log.Warnf("Failed to create sync task for new database %s (attempt %d/%d): %v, will retry",
						dbName, i+1, maxRetries, err)
					time.Sleep(time.Second * time.Duration(i+1)) // Exponential backoff
				}
			} else {
				log.Infof("Successfully created sync task for new database %s", dbName)
				// 记录数据库任务信息到monitor_db_job表
				jobName := fmt.Sprintf("%s_%s", request.Name, dbName)
				if err := m.db.AddMonitorJob(dbName, jobName, request.Name, true); err != nil {
					log.Warnf("Failed to add monitor job for new database %s: %v", dbName, err)
				}
				break
			}
		}
	}
}

// handleDeletedDatabases 处理已删除的数据库
func (m *DatabaseMonitor) handleDeletedDatabases(deletedDatabases []string, request *CreateCcrRequest) {
	if len(deletedDatabases) == 0 {
		return
	}

	log.Infof("Found %d deleted databases for task %s: %v", len(deletedDatabases), request.Name, deletedDatabases)

	for _, dbName := range deletedDatabases {
		if dbName == "" {
			log.Warnf("Skipping empty database name")
			continue
		}

		jobName := fmt.Sprintf("%s_%s", request.Name, dbName)

		maxRetries := 3
		for i := 0; i < maxRetries; i++ {
			if err := m.jobManager.RemoveJob(jobName); err != nil {
				if i == maxRetries-1 {
					log.Warnf("Failed to remove sync task for deleted database %s (attempt %d/%d): %v", dbName, i+1, maxRetries, err)
				} else {
					log.Warnf("Failed to remove sync task for deleted database %s (attempt %d/%d): %v, will retry", dbName, i+1, maxRetries, err)
					time.Sleep(time.Second * time.Duration(i+1)) // Exponential backoff
				}
			} else {
				log.Infof("Successfully removed sync task for deleted database %s", dbName)

				// 从monitor_db_job表中删除记录
				jobName := fmt.Sprintf("%s_%s", request.Name, dbName)
				if err := m.db.AddMonitorJob(dbName, jobName, request.Name, false); err != nil {
					log.Warnf("Failed to remove monitor job for deleted database %s: %v", dbName, err)
				} else {
					log.Infof("Successfully removed monitor job for deleted database %s", dbName)
				}

				break
			}
		}
	}
}

// logMonitoringStatus 记录监控状态
func (m *DatabaseMonitor) logMonitoringStatus(newDatabases []string, deletedDatabases []string, existingDatabases map[string]bool, taskName string) {
	if len(newDatabases) == 0 && len(deletedDatabases) == 0 {
		log.Infof("No database changes detected for task %s, currently have %d databases", taskName, len(existingDatabases))
	}
}
