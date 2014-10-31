CREATE DATA OBJECT application.app_debug_mode boolean DEFAULT false;
CREATE DATA OBJECT application.global_message_id_counter integer DEFAULT 0;
CREATE DATA OBJECT application.app_debug_log
collection (
	id	integer,
    moment string,
    message string,
    PRIMARY KEY (id)
);
