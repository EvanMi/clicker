CREATE TABLE click_records (
    id BIGINT NOT NULL PRIMARY KEY,
    receiver_id BIGINT NOT NULL,
    clicker_id BIGINT NOT NULL,
    click_type INT NOT NULL,
    created DATETIME NOT NULL,
    INDEX idx_receiver_clicker_created (receiver_id, clicker_id, created)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE `click_summary` (
  `id` bigint(20) NOT NULL,
  `receiver_id` bigint(20) NOT NULL,
  `like_count` bigint(20) NOT NULL,
  `created` datetime NOT NULL,
  `modified` datetime NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `uniq_receiver_id` (`receiver_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;