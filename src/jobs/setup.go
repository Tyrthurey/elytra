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

	manager.RegisterJobType(curseforgeJobTypeDownload, func(data map[string]interface{}) (Job, error) {
		return NewCurseForgeDownloadJob(data, serverManager)
	})

	manager.RegisterJobType(curseforgeJobTypeScan, func(data map[string]interface{}) (Job, error) {
		return NewCurseForgeScanJob(data, serverManager)
	})

	manager.RegisterJobType(curseforgeJobTypeDownloadBulk, func(data map[string]interface{}) (Job, error) {
		return NewCurseForgeBulkDownloadJob(data, serverManager)
	})

	manager.RegisterJobType(curseforgeJobTypeUpdate, func(data map[string]interface{}) (Job, error) {
		return NewCurseForgeUpdateJob(data, serverManager)
	})

	manager.RegisterJobType(curseforgeJobTypeRemove, func(data map[string]interface{}) (Job, error) {
		return NewCurseForgeRemoveJob(data, serverManager)
	})

	manager.RegisterJobType(curseforgeJobTypeUploadBlocked, func(data map[string]interface{}) (Job, error) {
		return NewCurseForgeUploadBlockedJob(data, serverManager)
	})

}
