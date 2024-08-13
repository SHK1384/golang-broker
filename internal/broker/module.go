package broker

import (
	"context"
	"sync"
	"therealbroker/pkg/broker"
	"therealbroker/pkg/pg"
	"time"
)

type MessageDate struct {
	message     *broker.Message
	subject     string
	currentTime time.Time
}

type Module struct {
	available             bool
	muCurrentChannelsList sync.Mutex
	muDatabase            sync.Mutex
	messagesChannels      map[string][]chan broker.Message
	db                    pg.Postgres
	dbType                string
}

func NewModule() broker.Broker {
	db, err := pg.NewPG(context.Background(), "postgres://postgres:shk138484@localhost:5432/broker")
	if err != nil {
		panic(err)
	}
	return &Module{available: true, messagesChannels: make(map[string][]chan broker.Message), db: *db, dbType: "postgresql"}
}

func (m *Module) Close() error {
	if m.available {
		m.available = false
		return nil
	}
	return broker.ErrUnavailable
}

func (m *Module) Publish(ctx context.Context, subject string, msg broker.Message) (int, error) {
	if !m.available {
		return -1, broker.ErrUnavailable
	}
	m.muCurrentChannelsList.Lock()
	currentChannelsList := m.messagesChannels[subject]
	m.muCurrentChannelsList.Unlock()
	for _, chn := range currentChannelsList {
		chn <- msg
	}
	if m.dbType == "postgresql" {
		m.muDatabase.Lock()
		err := m.db.InsertMessage(context.Background(), msg.Body, int(msg.Expiration.Seconds()), time.Now())
		m.muDatabase.Unlock()
		if err != nil {
			return -1, err
		}
		return -1, nil
	} else {
		// pass
	}
	return -1, nil
}

func (m *Module) Subscribe(ctx context.Context, subject string) (<-chan broker.Message, error) {
	if !m.available {
		return nil, broker.ErrUnavailable
	}
	m.muCurrentChannelsList.Lock()
	out := make(chan broker.Message, 10000)
	m.messagesChannels[subject] = append(m.messagesChannels[subject], out)
	m.muCurrentChannelsList.Unlock()
	return out, nil
}

func (m *Module) Fetch(ctx context.Context, subject string, id int) (broker.Message, error) {
	if !m.available {
		return broker.Message{}, broker.ErrUnavailable
	}
	m.muDatabase.Lock()
	body, expireDuration, currentTime, err := m.db.FetchMessage(context.Background(), id)
	m.muDatabase.Unlock()
	if err != nil {
		return broker.Message{}, nil
	}
	if currentTime.Year() == 1 {
		return broker.Message{}, broker.ErrInvalidID
	}
	if time.Since(currentTime) < time.Second*time.Duration(expireDuration) {
		msg := broker.Message{Body: body, Expiration: time.Second * time.Duration(expireDuration)}
		return msg, nil
	}
	return broker.Message{}, broker.ErrExpiredID
}
