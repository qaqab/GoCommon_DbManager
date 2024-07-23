package GoCommon_DbManager

import (
	"github.com/qaqab/GoCommon_File"

	"database/sql"
	"fmt"
	"strings"

	"github.com/elastic/go-elasticsearch/v7"
	"github.com/go-redis/redis"
	_ "github.com/go-sql-driver/mysql"
	"github.com/xanzy/go-gitlab"
)

type EsSettingDatas struct {
	Addresse string
	Username string
	Password string
}
type GitlabSettingDatas struct {
	Token     string
	GithubUrl string
	Username  string
	Password  string
}
type RedisSettingDatas struct {
	Addresse string
	Password string
	DB       int
}
type MysqlSettingDatas struct {
	Addresse string
	Username string
	Password string
	DB       string
}
type ClientAll struct {
	ConfigSetting struct {
		ConfigPath string
		ConfigName string
	}
	EsClient          *elasticsearch.Client `json:"EsClient"`
	EsSettingData     EsSettingDatas        `json:"EsSettingData"`
	GitClient         *gitlab.Client        `json:"GitlabClient"`
	GitlabSettingData GitlabSettingDatas    `json:"GitlabSettingDatas"`
	RedisClient       *redis.Client         `json:"RedisClient"`
	RedisSettingData  RedisSettingDatas     `json:"RedisSettingData"`
	MysqlClient       *sql.DB               `json:"MysqlClient"`
	MysqlSettingData  MysqlSettingDatas     `json:"MysqlSettingData"`
}

// DbManagerClient 根据传入的clientType参数，初始化ClientAll结构体中的对应客户端
// 参数：
//
//	clientAll *ClientAll - 需要被初始化的ClientAll结构体指针
//	clientType string - 需要初始化的客户端类型，如"es.default"、"gitlab.default"或"redis.default"
//
// 返回值：无
func (clientAll *ClientAll) DbManagerClient(clientType string) {
	viper := GoCommon_File.YamlConfig(clientAll.ConfigSetting.ConfigPath, clientAll.ConfigSetting.ConfigName)

	if strings.Split(clientType, ".")[0] == "es" {
		esAddr, _ := viper.Get(clientType + ".Addresses").(string)
		esUser, _ := viper.Get(clientType + ".Username").(string)
		esPass, _ := viper.Get(clientType + ".Password").(string)
		clientAll.EsSettingData.Addresse = esAddr
		clientAll.EsSettingData.Username = esUser
		clientAll.EsSettingData.Password = esPass
		es_Client := clientAll.GetEsClient()
		clientAll.EsClient = es_Client

	} else if strings.Split(clientType, ".")[0] == "gitlab" {
		token, _ := viper.Get(clientType + ".Token").(string)
		url, _ := viper.Get(clientType + ".Url").(string)
		username, _ := viper.Get(clientType + ".Username").(string)
		password, _ := viper.Get(clientType + ".Password").(string)

		clientAll.GitlabSettingData.GithubUrl = url
		clientAll.GitlabSettingData.Token = token
		clientAll.GitlabSettingData.Username = username
		clientAll.GitlabSettingData.Password = password
		git_Client := clientAll.GetGitlabClient()
		clientAll.GitClient = git_Client

	} else if strings.Split(clientType, ".")[0] == "redis" {
		redis_addr, _ := viper.Get(clientType + ".Addresses").(string)
		redis_password, _ := viper.Get(clientType + ".Password").(string)
		redis_db, _ := viper.Get(clientType + ".DB").(int)
		clientAll.RedisSettingData.Addresse = redis_addr
		clientAll.RedisSettingData.Password = redis_password
		clientAll.RedisSettingData.DB = redis_db
		redis_Client := clientAll.GetRedisClient()
		clientAll.RedisClient = redis_Client

	} else if strings.Split(clientType, ".")[0] == "mysql" {
		mysql_addr, _ := viper.Get(clientType + ".Addresses").(string)
		mysql_user, _ := viper.Get(clientType + ".Username").(string)
		mysql_password, _ := viper.Get(clientType + ".Password").(string)
		mysql_db, _ := viper.Get(clientType + ".DB").(string)
		clientAll.MysqlSettingData.Addresse = mysql_addr
		clientAll.MysqlSettingData.Username = mysql_user
		clientAll.MysqlSettingData.Password = mysql_password
		clientAll.MysqlSettingData.DB = mysql_db
		mysql_Client := clientAll.GetMysqlClient()
		clientAll.MysqlClient = mysql_Client

	}
}

func (clientAll ClientAll) GetGitlabClient() *gitlab.Client {
	// 使用给定的 token 和基础 URL 创建 Gitlab 客户端
	git_Client, err := gitlab.NewClient(clientAll.GitlabSettingData.Token, gitlab.WithBaseURL(clientAll.GitlabSettingData.GithubUrl))
	if err != nil {
		// 如果创建客户端出现错误，打印错误信息并触发 panic
		fmt.Printf("initclienterr:%v\n", err)
		panic(err)
	} else {
		// 客户端初始化成功，打印初始化完成信息
		fmt.Println("初始化完成")
	}
	// 返回 Gitlab 客户端对象
	return git_Client
}

func (clientAll ClientAll) GetEsClient() *elasticsearch.Client {

	cfg := elasticsearch.Config{
		Addresses: []string{clientAll.EsSettingData.Addresse},
		Username:  clientAll.EsSettingData.Username,
		Password:  clientAll.EsSettingData.Password,
	}
	ESClient, err := elasticsearch.NewClient(cfg)
	if err != nil {
		fmt.Printf("连接es错误:%v\n", err)
		panic(err)
	} else {
		fmt.Println("初始化完成")
	}
	return ESClient
}

func (clientAll ClientAll) GetRedisClient() *redis.Client {
	// 打印连接测试信息
	fmt.Println("Go Redis Connection Test")

	// 根据配置信息创建Redis客户端
	client := redis.NewClient(&redis.Options{
		Addr:     clientAll.RedisSettingData.Addresse,
		Password: clientAll.RedisSettingData.Password,
		DB:       clientAll.RedisSettingData.DB,
	})

	// 发送Ping命令测试连接
	_, err := client.Ping().Result()
	if err != nil {
		// 连接失败，打印错误信息并抛出异常
		fmt.Printf("连接redis出错,错误信息：%v", err)
		panic(err)
	} else {
		// 连接成功，打印成功信息
		fmt.Println("成功连接redis")
	}

	// 返回Redis客户端对象
	return client
}

func (clientAll ClientAll) GetMysqlClient() *sql.DB {
	mysql_dataSourceName := fmt.Sprintf("%s:%s@tcp(%s)/%s", clientAll.MysqlSettingData.Username, clientAll.MysqlSettingData.Password, clientAll.MysqlSettingData.Addresse, clientAll.MysqlSettingData.DB)
	DB, _ := sql.Open("mysql", mysql_dataSourceName)

	//设置数据库最大连接数
	DB.SetConnMaxLifetime(100)
	//设置上数据库最大闲置连接数
	DB.SetMaxIdleConns(10)

	//验证连接
	if err := DB.Ping(); err != nil {
		fmt.Printf("连接mysql出错,错误信息：%v", err)
		panic(err)
	}
	fmt.Println("成功连接mysql")
	return DB
}
