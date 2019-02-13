USE katastr_db;

CREATE INDEX idx_jmeno_vlastnika
  ON vlastnici (jmeno);

CREATE INDEX idx_cislo_lv
  ON lv (cislo_ku, cislo_lv);

CREATE INDEX idx_id_ulohy
  ON log_lv (id_ulohy DESC, cislo_lv DESC);

CREATE INDEX idx_id_scraping_stav
  ON log_uloha (id_scrapingu, stav);

CREATE INDEX idx_stav_rizeni
  ON log_rizeni (stav);

CREATE INDEX idx_posledni_rizeni
  ON log_rizeni (id DESC);

CREATE INDEX idx_rizeni
  ON rizeni (cislo_pracoviste, cislo_rizeni);

CREATE INDEX idx_seznam_nemovitosti_na_lv
  ON seznam_nemovitosti (id_lv, id_rizeni);

CREATE INDEX idx_zpracovane_lv
  ON log_lv (cislo_ku, cislo_lv DESC);


/*indexes by FK*/
/*
CREATE INDEX idx_pozemek_na_lv
  ON pozemek (id_lv);

CREATE INDEX idx_stavebni_objekt_na_lv
  ON stavebni_objekt (id_lv);

CREATE INDEX idx_stavba_na_lv
  ON stavba (id_lv);

CREATE INDEX idx_jednotka_na_lv
  ON jednotka (id_lv);

CREATE INDEX idx_vlastnici_ref
  ON vlastnici (id_lv, id_ref, typ_ref);
  */
