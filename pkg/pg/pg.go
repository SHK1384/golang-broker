package pg

import (
	"context"
	"fmt"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

type Postgres struct {
	db *pgxpool.Pool
}

var (
	pgInstance *Postgres
	pgOnce     sync.Once
)

func NewPG(ctx context.Context, connString string) (*Postgres, error) {
	pgOnce.Do(func() {
		db, err := pgxpool.New(ctx, connString)
		if err != nil {
			panic(fmt.Errorf("unable to insert row: %w", err))
		}
		pgInstance = &Postgres{db}
	})
	return pgInstance, nil
}

func (pg *Postgres) Close() {
	pg.db.Close()
}

func (pg *Postgres) InsertMessage(ctx context.Context, body string, expireDuration int, currentTime time.Time) (int, error) {
	query := `INSERT INTO message (body, expire_duration, cur_time) VALUES (@body, @expire_duration, @cur_time) returning id`
	args := pgx.NamedArgs{
		"body":            body,
		"expire_duration": expireDuration,
		"cur_time":        currentTime,
	}
	rows, err := pg.db.Query(ctx, query, args)
	if err != nil {
		return -1, fmt.Errorf("unable to insert row: %w", err)
	}
	var id int
	pgx.ForEachRow(rows, []any{&id}, func() error {
		return nil
	})
	return id, nil
}

func (pg *Postgres) FetchMessage(ctx context.Context, id int) (string, int, time.Time, error) {
	query := `SELECT * FROM message WHERE id = @id`
	args := pgx.NamedArgs{
		"id": id,
	}
	rows, err := pg.db.Query(ctx, query, args)
	if err != nil {
		return "", -1, time.Time{}, fmt.Errorf("unable to insert row: %w", err)
	}
	var (
		returnedId     int
		body           string
		expireDuration int
		currentTime    pgtype.Timestamptz
	)
	pgx.ForEachRow(rows, []any{&returnedId, &body, &expireDuration, &currentTime}, func() error {
		return nil
	})
	return body, expireDuration, currentTime.Time, nil
}
