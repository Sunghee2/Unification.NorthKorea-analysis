CREATE USER 'spark'@'localhost' IDENTIFIED BY 비밀번호;
CREATE USER 'zeppelin'@'localhost' IDENTIFIED BY 비밀번호;

CREATE DATABASE IF NOT EXISTS tweets;
USE tweets;

CREATE TABLE top_words ( 
    tw_date DATE NOT NULL, 
    word VARCHAR(16) NOT NULL, 
    count INT NOT NULL, 
    sentiment VARCHAR(5), 
    PRIMARY KEY (tw_date,word) 
);

GRANT ALL PRIVILEGES ON tweets.* TO 'spark'@'localhost';
GRANT SELECT ON tweets.* TO 'zeppelin'@'localhost';
FLUSH PRIVILEGES;