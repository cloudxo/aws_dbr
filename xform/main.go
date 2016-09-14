package main

import (
    "os"
    // "log"
    "github.com/urfave/cli"
)

func main() {
    config := Config{}

    app := cli.NewApp()
    app.Name = "berg"
    app.Usage = "berg SOURCE DEST"
    app.Version = "0.9.0"

    app.Flags = []cli.Flag{
        cli.StringFlag{
            Name:  "access-key",
            Usage: "AWS Access Key `ACCESS_KEY`",
            EnvVar: "AWS_ACCESS_KEY_ID,AWS_ACCESS_KEY",
        },
        cli.StringFlag{
            Name:  "secret-key",
            Usage: "AWS Secret Key `SECRET_KEY`",
            EnvVar: "AWS_SECRET_ACCESS_KEY,AWS_SECRET_KEY",
        },
        cli.StringFlag{
            Name:  "token",
            Usage: "AWS Access Token `AWS_TOKEN`",
            EnvVar: "AWS_SESSION_TOKEN",
        },
        cli.BoolFlag{
            Name:  "partition",
            Usage: "Partition the data into YYYYMMDD tables",
        },
    }

    app.Action = func(c *cli.Context) error {
        args := c.Args()

        if len(args) != 2 {
            return cli.NewExitError("Missing required arguments", 1)
        }

        Process(config, args.Get(0), args.Get(1), c.Bool("partition"))
        return nil
    }

    app.Run(os.Args)
}
