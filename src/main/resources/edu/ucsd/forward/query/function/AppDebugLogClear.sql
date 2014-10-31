CREATE ACTION app_debug_log_clear()
AS BEGIN
	DELETE FROM application.app_debug_log;
END;