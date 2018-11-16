CREATE DATABASE IF NOT EXISTS katastr_db
  CHARACTER SET utf8 COLLATE utf8_general_ci;

USE katastr_db;

-- SELECT CONCAT('DROP TABLE ', GROUP_CONCAT(table_name), ';') AS query
--   FROM INFORMATION_SCHEMA.TABLES
--   WHERE TABLE_ROWS = '0'
--     AND TABLE_SCHEMA = 'katastr';

-- CREATE TABLE IF NOT EXISTS


CREATE OR REPLACE TABLE ku (
  cislo_ku MEDIUMINT UNSIGNED NOT NULL PRIMARY KEY,
  nazev_ku VARCHAR(100) NOT NULL,
  cislo_obce VARCHAR(100),
  plati_od DATE,
  plati_do DATE
);


CREATE OR REPLACE TABLE lv (
  id INT NOT NULL AUTO_INCREMENT,
  cislo_zaznamu INT NOT NULL,
  cislo_lv MEDIUMINT UNSIGNED NOT NULL,
  cislo_ku MEDIUMINT UNSIGNED NOT NULL,
  prava_stavby TEXT,
  datum_zmeny DATETIME DEFAULT CURRENT_TIMESTAMP,
  bylo_vymazano BOOLEAN,
  UNIQUE KEY unikatni_lv (cislo_lv, cislo_ku),
  CONSTRAINT PK_lv PRIMARY KEY (id,cislo_zaznamu)
);

CREATE OR REPLACE TABLE pozemek (
  ext_id_parcely BIGINT NOT NULL,
  cislo_zaznamu INT NOT NULL,
  id_lv INT NOT NULL,
  parcelni_cislo VARCHAR(100), -- nemusi byt cislo
  obec VARCHAR(100),
  cislo_obce INT UNSIGNED,
  vymera MEDIUMINT UNSIGNED,
  typ_parcely VARCHAR(100),
  druh_pozemku VARCHAR(100),
  cislo_stavebniho_objektu INT,
  zpusob_ochrany_nemovitosti TEXT,
  omezeni_vlastnickeho_prava TEXT,
  jine_zapisy TEXT,
  datum_zmeny DATETIME DEFAULT CURRENT_TIMESTAMP,
  bylo_vymazano BOOLEAN,
  CONSTRAINT PK_pozemek PRIMARY KEY (ext_id_parcely,cislo_zaznamu)
);

CREATE OR REPLACE TABLE stavebni_objekt (
  id INT NOT NULL AUTO_INCREMENT,
  cislo_zaznamu INT NOT NULL,
  id_lv INT NOT NULL,
  ext_id_parcely BIGINT UNSIGNED NOT NULL,
  cisla_popis_evid VARCHAR(50),
  typ VARCHAR(50),
  zpusob_vyuziti VARCHAR(100),
  datum_dokonceni DATE,
  pocet_bytu SMALLINT UNSIGNED,
  zastavena_plocha MEDIUMINT UNSIGNED,
  podlahova_plocha MEDIUMINT UNSIGNED,
  pocet_podlazi SMALLINT UNSIGNED,
  ext_id_stavebniho_objektu BIGINT,
  datum_zmeny DATETIME DEFAULT CURRENT_TIMESTAMP,
  bylo_vymazano BOOLEAN,
  CONSTRAINT PK_stavebni_objekt PRIMARY KEY (id,cislo_zaznamu)
);

CREATE OR REPLACE TABLE stavba (
  id INT NOT NULL AUTO_INCREMENT,
  cislo_zaznamu INT NOT NULL,
  id_lv INT NOT NULL,
  obec VARCHAR(100),
  cislo_obce INT UNSIGNED,
  cast_obce VARCHAR(100),
  cislo_casti_obce INT UNSIGNED,
  typ_stavby VARCHAR(100),
  zpusob_vyuziti VARCHAR(100),
  stoji_na_pozemku VARCHAR(100),
  ext_id_stavebniho_objektu BIGINT,
  datum_zmeny DATETIME DEFAULT CURRENT_TIMESTAMP,
  bylo_vymazano BOOLEAN,
  CONSTRAINT PK_stavba PRIMARY KEY (id,cislo_zaznamu)
);

CREATE OR REPLACE TABLE jednotka (
  id INT NOT NULL AUTO_INCREMENT,
  cislo_zaznamu INT NOT NULL,
  id_lv INT NOT NULL,
  cislo_jednotky VARCHAR(30),
  typ_jednotky VARCHAR(100),
  zpusob_vyuziti VARCHAR(100),
  podil_na_spol_castech VARCHAR(20),
  zpusob_ochrany_nemovitosti TEXT,
  omezeni_vlastnickeho_prava TEXT,
  jine_zapisy TEXT,
  datum_zmeny DATETIME DEFAULT CURRENT_TIMESTAMP,
  bylo_vymazano BOOLEAN,
  CONSTRAINT PK_jednotka PRIMARY KEY (id,cislo_zaznamu)
);


CREATE OR REPLACE TABLE vlastnici (
  id INT NOT NULL AUTO_INCREMENT,
  cislo_zaznamu INT NOT NULL,
  id_lv INT NOT NULL,
  id_ref BIGINT UNSIGNED NOT NULL,
  typ_ref VARCHAR(20) NOT NULL,
  vlastnicke_pravo VARCHAR(400),
  jmeno VARCHAR(200),
  adresa VARCHAR(200),
  podil VARCHAR(20),
  datum_zmeny DATETIME DEFAULT CURRENT_TIMESTAMP,
  bylo_vymazano BOOLEAN,
  CONSTRAINT PK_vlastnici PRIMARY KEY (id,cislo_zaznamu)
);


CREATE OR REPLACE TABLE rizeni (
  id INT NOT NULL PRIMARY KEY AUTO_INCREMENT,
  cislo_pracoviste MEDIUMINT UNSIGNED NOT NULL,
  cislo_rizeni VARCHAR(15) NOT NULL,
  cislo_ku MEDIUMINT UNSIGNED,
  -- id_lv INT, referencia na LV je priamo v sezname nemovitosti
  datum_prijeti DATETIME,
  stav_rizeni VARCHAR(50),
  datum_upravy TIMESTAMP,
  UNIQUE KEY unikatni_rizeni (cislo_pracoviste, cislo_rizeni)
);

CREATE OR REPLACE TABLE ucastnici_rizeni (
  id_rizeni INT NOT NULL,
  poradove_cislo SMALLINT UNSIGNED NOT NULL,
  jmeno VARCHAR(200),
  typ VARCHAR(50),
  CONSTRAINT PK_ucastnici_rizeni PRIMARY KEY (id_rizeni,poradove_cislo)
);

CREATE OR REPLACE TABLE provedene_operace (
  id_rizeni INT NOT NULL,
  poradove_cislo SMALLINT UNSIGNED NOT NULL,
  operace VARCHAR(50),
  datum DATE,
  CONSTRAINT PK_provedene_operace PRIMARY KEY (id_rizeni,poradove_cislo)
);

CREATE OR REPLACE TABLE predmety_rizeni (
  id_rizeni INT NOT NULL,
  poradove_cislo SMALLINT UNSIGNED NOT NULL,
  typ VARCHAR(50),
  CONSTRAINT PK_predmet_rizeni PRIMARY KEY (id_rizeni,poradove_cislo)
);

CREATE OR REPLACE TABLE seznam_nemovitosti (
  id_rizeni INT NOT NULL,
  poradove_cislo SMALLINT UNSIGNED NOT NULL,
  id_lv INT,
  typ VARCHAR(20),
  cislo VARCHAR(20),
  CONSTRAINT PK_seznam_nemovitosti PRIMARY KEY (id_rizeni,poradove_cislo)
);


CREATE OR REPLACE TABLE log_scraping (
  id INT NOT NULL PRIMARY KEY AUTO_INCREMENT,
  datum_zacatku DATETIME DEFAULT CURRENT_TIMESTAMP,
  nazev VARCHAR(100)
);

CREATE OR REPLACE TABLE log_uloha (
  id INT NOT NULL PRIMARY KEY AUTO_INCREMENT,
  id_scrapingu INT NOT NULL,
  cislo_ku MEDIUMINT UNSIGNED NOT NULL,
  hash_ulohy CHAR(32),
  stav CHAR(1), -- W (waiting to process), R (running), F (finished), E (error)
  datum DATETIME DEFAULT CURRENT_TIMESTAMP,
  datum_konce DATETIME
);

CREATE OR REPLACE TABLE log_lv (
  id INT NOT NULL PRIMARY KEY AUTO_INCREMENT,
  id_ulohy INT,
  cislo_lv MEDIUMINT UNSIGNED NOT NULL,
  cislo_ku MEDIUMINT UNSIGNED NOT NULL,
  datum DATETIME DEFAULT CURRENT_TIMESTAMP,
  existuje BOOLEAN
);

CREATE OR REPLACE TABLE log_rizeni (
  id INT NOT NULL PRIMARY KEY AUTO_INCREMENT,
  pracoviste INT,
  typ CHAR(1),
  datum DATE,
  stav CHAR(1), -- R (running), F (finished)
  datum_zalozeni DATETIME DEFAULT CURRENT_TIMESTAMP
);
