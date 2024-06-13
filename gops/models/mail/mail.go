package mail

import (
	"net/smtp"
	"strings"

	"github.com/astaxie/beego"
)

type Email struct {
	to      string "to"
	subject string "subject"
	msg     string "msg"
}

func NewEmail(to, subject, msg string) *Email {
	return &Email{to: to, subject: subject, msg: msg}
}

func SendEmail(email *Email) (users []string) {
	user := beego.AppConfig.String("MailUser")
	host := beego.AppConfig.String("MailHost")
	server_addr := beego.AppConfig.String("MailServerAddr")
	passwd := beego.AppConfig.String("MailPasswd")

	auth := smtp.PlainAuth("", user, passwd, host)
	sendTo := strings.Split(email.to, ";")
	done := make(chan error, 1024)
	beego.Info(server_addr, " ", host, " ", user, " ", passwd)
	go func() {
		defer close(done)
		for _, v := range sendTo {

			str := strings.Replace("From: "+user+"~To: "+v+"~Subject: "+email.subject+"~~", "~", "\r\n", -1) + email.msg

			err := smtp.SendMail(
				server_addr,
				auth,
				user,
				[]string{v},
				[]byte(str),
			)
			if err == nil {
				users = append(users, v)
			}

			done <- err
		}
	}()

	for i := 0; i < len(sendTo); i++ {
		<-done
	}

	return
}
