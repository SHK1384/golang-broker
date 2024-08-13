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
	db           *pgxpool.Pool
	currentBatch *pgx.Batch
	batchCount   int
	batchLimit   int
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
		pgInstance = &Postgres{db: db, currentBatch: &pgx.Batch{}, batchCount: 0, batchLimit: 100}
	})
	return pgInstance, nil
}

func (pg *Postgres) Close() {
	pg.db.Close()
}

func (pg *Postgres) InsertMessage(ctx context.Context, body string, expireDuration int, currentTime time.Time) error {
	query := `INSERT INTO message (body, expire_duration, cur_time) VALUES (@body, @expire_duration, @cur_time)`
	args := pgx.NamedArgs{
		"body":            body,
		"expire_duration": expireDuration,
		"cur_time":        currentTime,
	}
	pg.currentBatch.Queue(query, args)
	pg.batchCount++
	if pg.batchCount == pg.batchLimit {
		results := pg.db.SendBatch(ctx, pg.currentBatch)
		defer results.Close()
		for i := 0; i < pg.batchLimit; i++ {
			_, err := results.Exec()
			if err != nil {
				return err
			}
		}
		pg.currentBatch = &pgx.Batch{}
		pg.batchCount = 0
	}
	return nil
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
