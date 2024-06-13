package almrec

import (
	"kagamistoreage/gops/models/mail"
	"kagamistoreage/gops/models/user"
	"time"
)

var (
	Alarm_string chan string = make(chan string, 5000)
)

func Alarm_demo() {
	for {
		select {
		case ch1 := <-Alarm_string:
			alarm_do(ch1)
			break
			//   fmt.Println(ch1)
		//设置5秒超时
		case <-time.After(5 * time.Second):
			//	fmt.Println("read timeout")
			break
		}
	}
}

func alarm_do(info string) {
	var (
		mails string
	)

	flag := make(map[string]string)
	flag["is_alarm"] = "1"
	users, err := user.UsersbyCon(flag)
	if err != nil {
		return
	}

	for _, us := range users {

		if len(mails) == 0 {
			mails = us.Mail
		} else {
			mails = mails + ";" + us.Mail
		}
	}
	if len(mails) == 0 {
		return
	}

	email := mail.NewEmail(mails, "efs alarm", info)
	smails := mail.SendEmail(email)

	for _, e := range smails {
		for _, us := range users {
			if us.Mail == e {
				Add_Almrec(info, time.Now().Format("2006-01-02 15:04:05"), e)
				//insert alarm sql
			}
		}
	}

}
