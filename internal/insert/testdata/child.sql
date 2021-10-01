DROP DATABASE IF EXISTS test;
CREATE DATABASE test;

CREATE TABLE `test`.`child` (
  `child_id` int NOT NULL AUTO_INCREMENT,
  `user_name` varchar(45) NOT NULL,
  `password` varchar(255) NOT NULL,
  `parent_id` int NOT NULL,
  `avatar` varchar(255) NOT NULL,
  `total_balance` decimal(10,2) NOT NULL DEFAULT '0.00',
  `available_cups` int NOT NULL DEFAULT '0',
  `sold_cups` int NOT NULL DEFAULT '0',
  `total_sales` decimal(10,2) NOT NULL DEFAULT '0.00',
  `last_seen` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`child_id`),
  UNIQUE KEY `username_idx` (`user_name`),
  KEY `parent_id_isx` (`parent_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE `test`.`parent` (
  `parent_id` int NOT NULL AUTO_INCREMENT,
  `first_name` varchar(45) NOT NULL,
  `last_name` varchar(45) NOT NULL,
  `email` varchar(45) NOT NULL,
  `opt_in` tinyint(1) NOT NULL,
  `avatar` int NOT NULL,
  `password` varchar(255) NOT NULL,
  `registered_with_google` tinyint(1) DEFAULT NULL,
  `registered_with_facebook` tinyint(1) DEFAULT NULL,
  `recovery_token` varchar(255) DEFAULT NULL,
  `verified` tinyint(1) DEFAULT NULL,
  `accepted_terms` datetime DEFAULT NULL,
  `zip` varchar(10) NOT NULL,
  PRIMARY KEY (`parent_id`),
  UNIQUE KEY `parent_email_idx` (`email`),
  KEY `recovery_token_idx` (`recovery_token`),
  KEY `zip` (`zip`)
) ENGINE=InnoDB AUTO_INCREMENT=41 DEFAULT CHARSET=utf8;
