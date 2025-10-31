-- DDL para tablas destino

CREATE TABLE IF NOT EXISTS regulations (
  id SERIAL PRIMARY KEY,
  created_at TIMESTAMP NULL,
  update_at TIMESTAMP NULL,
  is_active BOOLEAN,
  title VARCHAR(255),
  gtype VARCHAR(50),
  entity VARCHAR(255),
  external_link TEXT,
  rtype_id INTEGER,
  summary TEXT,
  classification_id INTEGER
);

CREATE TABLE IF NOT EXISTS regulations_component (
  id SERIAL PRIMARY KEY,
  regulations_id INTEGER REFERENCES regulations(id),
  components_id INTEGER
);