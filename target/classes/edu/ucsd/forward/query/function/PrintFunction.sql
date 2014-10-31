CREATE FUNCTION print(message string)
AS BEGIN
	IF application.app_debug_mode THEN
		INSERT INTO application.app_debug_log(id, moment, message) VALUES(application.global_message_id_counter, current_timestamp(), message);
	END IF;
	set application.global_message_id_counter = application.global_message_id_counter + 1;
END ;