package sqlite

import (
	"database/sql"
	"fmt"
	"url-shortener/internal/storage"

	_ "modernc.org/sqlite" // init sqlite driver
)

type Storage struct {
	db *sql.DB
}

func New(storagePath string) (*Storage, error) {
	const fn = "storage.sqlite.New"
	
	db, err := sql.Open("sqlite", storagePath)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", fn, err)
	}

	if err := db.Ping(); err != nil {
		db.Close()
		return nil, fmt.Errorf("%s: failed to ping database: %w", fn, err)
	}

	stmt, err := db.Prepare(`
	CREATE TABLE IF NOT EXISTS url(
		id INTEGER PRIMARY KEY,
		alias TEXT NOT NULL UNIQUE,
		url TEXT NOT NULL
	);
	CREATE INDEX IF NOT EXISTS idx_alias ON url(alias);
	`)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", fn, err)
	}
	defer stmt.Close()

	_, err = stmt.Exec()
	if err != nil {
		return nil, fmt.Errorf("%s: %w", fn, err)
	}

	return &Storage{db: db}, nil
}

func (s *Storage) SaveURL(urlToSave, alias string) (int64, error) {
	const fn = "storage.sqlite.SaveURL"

	stmt, err := s.db.Prepare("INSERT INTO url(url, alias) VALUES(?, ?)")
	if err != nil {
		return 0, fmt.Errorf("%s: %w", fn, err)
	}

	res, err := stmt.Exec(urlToSave, alias)
	if err != nil {
		if driverErr, ok := err.(interface{ Number() int64 }); ok {
			if driverErr.Number() == 2067 || driverErr.Number() == 19 { 
				return 0, fmt.Errorf("%s: %w", fn, storage.ErrURLExists)
			}
		}

		return 0, fmt.Errorf("%s: %w", fn, err)
	}

	id, err := res.LastInsertId()
	if err != nil {
		return 0, fmt.Errorf("%s: failed to get last insert id: %w", fn, err)
	}

	return id, nil
}

func (s *Storage) GetURL(alias string) (string, error) {
	const fn = "storage.sqlite.GetURL"

	stmt, err := s.db.Prepare("SELECT url FROM url WHERE alias = ?")
	if err != nil {
		return "", fmt.Errorf("%s: prepare statement: %w", fn, err)
	}

	var receivedUrl string
	err = stmt.QueryRow(alias).Scan(&receivedUrl)
	if err != nil {
		if err == sql.ErrNoRows {
			return "", fmt.Errorf("%s: %w", fn, storage.ErrURLNotFound)
		}
		return "", fmt.Errorf("%s: %w", fn, err)
	}

	return receivedUrl, nil
}

func (s *Storage) DeleteURL(alias string) error {
	const fn = "storage.sqlite.DeleteURL"

	stmt, err := s.db.Prepare("DELETE FROM url WHERE alias = ?")
	if err != nil {
		return fmt.Errorf("%s: failed to prepare statement: %w", fn, err)
	}

	res, err := stmt.Exec(alias)
	if err != nil {
		return fmt.Errorf("%s: failed to execute statement: %w", fn, err)
	}	

	affectedRows, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("%s: failed to get affected rows: %w", fn, err)
	}	
	if affectedRows == 0 {
		return fmt.Errorf("%s: %w", fn, storage.ErrURLNotFound)
	}

	return nil
}