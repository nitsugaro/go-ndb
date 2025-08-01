package ndb

type Middleware = func(query any) error

func (d *DBBridge) AddMiddleware(m Middleware) {
	d.middlewares = append(d.middlewares, m)
}

func (d *DBBridge) runMiddlewares(query any) error {
	for _, m := range d.middlewares {
		if err := m(query); err != nil {
			return err
		}
	}

	return nil
}
