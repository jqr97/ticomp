package main

import (
	"fmt"
	"net"
	"os"

	"github.com/go-mysql-org/go-mysql/server"
	"github.com/lonng/ticomp/config"
	"github.com/lonng/ticomp/handler"
	"github.com/spf13/cobra"
)

func main() {
	cfg := &config.Config{}
	cmd := &cobra.Command{
		Use:          "ticomp",
		SilenceUsage: true,
		RunE: func(cmd *cobra.Command, args []string) error {
			l, err := net.Listen("tcp", fmt.Sprintf(":%d", cfg.Port))
			if err != nil {
				return err
			}

			fmt.Printf("Serve successfully (mysql -h 127.0.0.1 -P %d -u%s -p)\n", cfg.Port, cfg.User)

			// Waiting for MySQL client connect
			for {
				c, err := l.Accept()
				if err != nil {
					return err
				}

				// Serve the new incoming connection
				go serveConn(cfg, c)
			}
		},
	}

	flags := cmd.Flags()
	flags.SortFlags = false

	// Shadow server configurations
	flags.IntVarP(&cfg.Port, "port", "P", 5001, "Listen port of TiCompare shadow server")
	flags.StringVar(&cfg.User, "user", "root", "TiCompare shadow server user name")
	flags.StringVar(&cfg.Pass, "pass", "", "TiCompare shadow server password")

	// MySQL server configurations
	flags.StringVar(&cfg.MySQL.Host, "mysql.host", "127.0.0.1", "MySQL server host name")
	flags.IntVar(&cfg.MySQL.Port, "mysql.port", 3306, "MySQL server port")
	flags.StringVar(&cfg.MySQL.User, "mysql.user", "root", "MySQL server user name")
	flags.StringVar(&cfg.MySQL.Pass, "mysql.pass", "", "MySQL server password")
	flags.StringVar(&cfg.MySQL.Name, "mysql.name", "", "MySQL server database name")
	flags.StringVar(&cfg.MySQL.Options, "mysql.options", "charset=utf8mb4", "MySQL server connection options")

	// TiDB server configurations
	flags.StringVar(&cfg.TiDB.Host, "tidb.host", "127.0.0.1", "TiDB server host name")
	flags.IntVar(&cfg.TiDB.Port, "tidb.port", 4000, "TiDB server port")
	flags.StringVar(&cfg.TiDB.User, "tidb.user", "root", "TiDB server user name")
	flags.StringVar(&cfg.TiDB.Pass, "tidb.pass", "", "TiDB server password")
	flags.StringVar(&cfg.TiDB.Name, "tidb.name", "", "TiDB server database name")
	flags.StringVar(&cfg.TiDB.Options, "tidb.options", "charset=utf8mb4", "TiDB server connection options")

	if err := cmd.Execute(); err != nil {
		fmt.Println("Execute command failed", err)
		os.Exit(2)
	}
}

func serveConn(cfg *config.Config, c net.Conn) {
	h := handler.NewShadowHandler(cfg)

	err := h.Initialize()
	if err != nil {
		fmt.Println("Initialize the database connection failed", err, c.RemoteAddr().String())
		return
	}

	defer func() {
		if err := h.Finalize(); err != nil {
			fmt.Println("Finalize shadow handler failed", err)
		}
	}()

	conn, err := server.NewConn(c, cfg.User, cfg.Pass, h)
	if err != nil {
		fmt.Println("Establish database connection failed", err, c.RemoteAddr().String())
		return
	}
	defer conn.Close()

	// as long as the client keeps sending commands, keep handling them
	for {
		if err := conn.HandleCommand(); err != nil {
			fmt.Println("Handle client command failed and the connection will be terminated", err)
			return
		}
	}
}
