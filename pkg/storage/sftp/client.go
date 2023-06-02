package sftp

import (
	"fmt"
	"time"

	"github.com/pkg/sftp"
	"golang.org/x/crypto/ssh"
)

type sftpClient struct {
	*sftp.Client
	conn *ssh.Client
}

func newClient(conf *sftpConfig) (*sftpClient, error) {
	var auths []ssh.AuthMethod
	if conf.Password != nil {
		auths = append(auths, ssh.Password(*conf.Password))
	}
	if conf.PrivateKey != nil {
		signer, err := ssh.ParsePrivateKey([]byte(*conf.PrivateKey))
		if err == nil {
			auths = append(auths, ssh.PublicKeys(signer))
		}
	}
	sshConf := &ssh.ClientConfig{
		User:            conf.User,
		Auth:            auths,
		HostKeyCallback: ssh.InsecureIgnoreHostKey(),
		Timeout:         15 * time.Second,
	}
	addr := fmt.Sprintf("%s:%d", conf.Host, conf.Port)
	conn, err := ssh.Dial("tcp", addr, sshConf)
	if err != nil {
		return nil, fmt.Errorf("failed to dial ssh: %s", err)
	}

	const maxPacket = 1 << 15
	c, err := sftp.NewClient(conn, sftp.MaxPacket(maxPacket))
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("failed to create sftp client: %s", err)
	}

	return &sftpClient{
		Client: c,
		conn:   conn,
	}, nil
}

func (c *sftpClient) Close() {
	if c.Client != nil {
		c.Client.Close()
		c.conn.Close()
	}
}
