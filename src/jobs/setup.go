package jobs

import (
	"github.com/pyrohost/elytra/src/remote"
	"github.com/pyrohost/elytra/src/server"
)

// SetupJobs registers all available job types with the manager
func SetupJobs(manager *Manager, serverManager *server.Manager, client remote.Client) {
	manager.RegisterJobType("backup_create", func(data map[string]interface{}) (Job, error) {
		return NewBackupCreateJob(data, serverManager, client)
	})

	manager.RegisterJobType("backup_delete", func(data map[string]interface{}) (Job, error) {
		return NewBackupDeleteJob(data, serverManager, client)
	})

	manager.RegisterJobType("backup_restore", func(data map[string]interface{}) (Job, error) {
		return NewBackupRestoreJob(data, serverManager, client)
	})

	manager.RegisterJobType("backup_delete_all", func(data map[string]interface{}) (Job, error) {
		return NewBackupDeleteAllJob(data, serverManager, client)
	})

	manager.RegisterJobType(modrinthJobTypeDownload, func(data map[string]interface{}) (Job, error) {
		return NewModrinthDownloadJob(data, serverManager)
	})

	manager.RegisterJobType(modrinthJobTypeScan, func(data map[string]interface{}) (Job, error) {
		return NewModrinthScanJob(data, serverManager)
	})

	manager.RegisterJobType(modrinthJobTypeDownloadBulk, func(data map[string]interface{}) (Job, error) {
		return NewModrinthBulkDownloadJob(data, serverManager)
	})

	manager.RegisterJobType(modrinthJobTypeUpdate, func(data map[string]interface{}) (Job, error) {
		return NewModrinthUpdateJob(data, serverManager)
	})

	manager.RegisterJobType(modrinthJobTypeRemove, func(data map[string]interface{}) (Job, error) {
		return NewModrinthRemoveJob(data, serverManager)
	})

}
