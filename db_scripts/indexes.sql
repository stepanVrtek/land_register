USE katastr_db;

CREATE INDEX idx_jmeno_vlastnika
  ON vlastnici (jmeno);

CREATE INDEX idx_cislo_lv
  ON lv (cislo_ku, cislo_lv);

CREATE INDEX idx_id_ulohy
  ON log_lv (id_ulohy DESC, cislo_lv DESC);
