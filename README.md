# gotree
<img align="right" width="230px" src="https://raw.githubusercontent.com/8treenet/blog/master/img/4_Grayscale_logo_on_transparent_1024.png">
gotree 是一个垂直分布式框架。 gotree 的目标是轻松开发分布式服务，解放开发者心智负担。

## 特性
* 熔断
* fork 热更新
* rpc 通信(c50k)
* 定时器
* SQL 慢查询监控
* SQL 冗余监控
* 分层
* 强制垂直分库
* 基于 gseq 串行的全网日志
* 单元测试
* 督程
* 一致性哈希、主从、随机、均衡等负载方式

## 介绍
- [快速使用](#快速使用)
- [描述](#描述)
- [分层](#分层)
- [Gateway](#gateway)
- [BusinessController](#business_controller)
- [BusinessService](#business_service)
- [BusinessCmd](#business_cmd)
- [DaoCmd](#dao_cmd)
- [ComController](#com_controller)
- [ComModel](#com_model)
- [事务](#com_controller)
- [进阶使用](#进阶使用)
- [Timer](#timer)
- [Helper](#helper)
- [ComCache](#cache)
- [ComMemory](#memory)
- [ComApi](#api)
- [配置文件](#helper)
- [单元测试](#unit)
- [命令](#command)
- [分布示例](#dispersed)


## 快速使用

1. 获取 gotree。
```sh
$ go get -u github.com/8treenet/gotree
```

2. 安装 gotree。
```sh
$ cd $GOPATH/src/github.com/8treenet/gotree/
$ go install
```

3. 创建 learning 项目。
```sh
$ gotree new learning
```

4. learning 项目数据库安装、数据库用户密码配置。使用 source 或工具安装 learning.sql。
```sh
$ mysql > source $GOPATH/src/learning/learning.sql
# 编辑 db 连接信息，Com = Order、用户名 = root、密码 = 123123、地址 = 127.0.0.1、端口 = 3306、数据库 = learning_order
# Order = "root:123123@tcp(127.0.0.1:3306)/learning_order?charset=utf8"
$ vi $GOPATH/src/learning/dao/conf/dev/db.conf
```

5. 启动 dao服务、 business 服务。
```sh
$ cd $GOPATH/src/learning/dao
$ go run main.go
$ command + t #开启新窗口
$ cd $GOPATH/src/learning/business
$ go run main.go
```

6. 模拟网关执行调用，请查看代码。 代码位于 $GOPATH/src/learning/business/unit/gateway_test.go
```sh
$ go test -v -count=1 -run TestUserRegister $GOPATH/src/learning/business/unit/gateway_test.go
$ go test -v -count=1 -run TestStore $GOPATH/src/learning/business/unit/gateway_test.go
$ go test -v -count=1 -run TestShopping $GOPATH/src/learning/business/unit/gateway_test.go
$ go test -v -count=1 -run TestUserOrder $GOPATH/src/learning/business/unit/gateway_test.go
```

7. qps 压测
```sh
# 每秒 1w 请求
$ go run $GOPATH/src/learning/business/unit/qps_press/main.go 10000
```

## 快速入门

### 描述
+ Business 主要用于逻辑功能处理等。均衡负载部署多台，为网关提供服务。 目录结构在 learning/business。
+ Dao 主要用于数据功能处理，组织低级数据提供给上游 business。Dao 基于容器设计，开发 Com 挂载不同的 Dao 容器上。负载均衡方式较多，可根据数据分布来设计。可通过配置来开启 Com(Component Object Model)。目录结构在 learning/dao。
+ Protocol 通信协议 business_cmd/value 作用于 Api网关和 Business 通信。 dso_cmd/value 作用于 Business 和 Dao 通信。 目录结构在 learning/protocol。

> 3台网关、2台business、3台dao 组成的集群
>
> 服务器 | 服务器 | 服务器
> -------------|-------------|-------------
> APIGateway-1 | APIGateway-2 | TcpGateway-1
> Business-1   |              | Business-2
> Dao-1        | Dao-2        | Dao-3

### 分层
架构主要分为4层。第一层基类 __BusinessController__，作为 Business 的入口控制器, 主要职责有组织和协调Service、逻辑处理。 第二层基类 __BusinessService__, 作为 __BusinessController__ 的下沉层， 主要下沉的职责有拆分、治理、解耦、复用、使用Dao。 第三层基类 __ComController__ ，作为 Dao 的入口控制器，主要职责有组织数据、解耦数据和逻辑、抽象数据源、使用数据源。 第四层多种基类 __ComModel__ 数据库表模型基类、 __ComMemory__ 内存基类、 __ComCache__ redis基类、 __ComApi__ Http数据基类。

### gateway
```go
/*  
    1. 模拟api网关调用，等同 beego、gin 等api gateway, 以及 tcp 网关项目.
    2. 实际应用中 business 分布在多个物理机器.  gateway.AppendBusiness 因填写多机器的内网ip.
*/
    func main() {
        gateway.AppendBusiness("192.168.1.1:8888")
        gateway.AppendBusiness("192.168.1.2:8888")
        gateway.AppendBusiness("192.168.1.3:8888")
        gateway.Run()
        
        //创建 business 调用命令
        cmd := new(business_cmd.Store).Gotree([]int64{1, 2})
        //创建 business 返回数据
        value := business_value.Store{}
        
        //设置自定义头，透传数据
        cmd.SetHeader("", "")
        //可直接设置http头
        cmd.SetHttpHeader(head) 
        gateway.RpcClient().Call(cmd, &value)
        //response value ....
	}
```

### business_controller
```go
    /* 
         learning/business/controllers/product_controller.go
    */
    func init() {
        //注册 ProductController 控制器
        business.RegisterController(new(ProductController).Gotree())
    }

    //定义一个电商的商品控制器。控制器命名 `Name`Controller
    type ProductController struct {
        //继承 business 控制器的基类
	    business.BusinessController
    }

    //这个是 gotree 风格的构造函数，底层通过指针原型链，可以实现多态，和基础类的支持。
    func (self *ProductController) Gotree() *ProductController {
        self.BusinessController.Gotree(self)
        return self
    }

    //每一个 APIGateway 触发的 rpc 动作调用，都会创造一个 ProductController 对象，并且调用 Prepare。
    func (self *ProductController) Prepare(method string, argv interface{}) {
        //调用父类 Prepare
        self.BusinessController.Prepare(method, argv)
        helper.Log().Notice("Prepare:", method, argv)
    }

    //每一个 APIGateway 触发的 rpc 动作调用结束 都会触发 Finish。
    func (self *ProductController) Finish(method string, reply interface{}, e error) {
        self.BusinessController.Finish(method, reply, e)
        //打印日志
        helper.Log().Notice("Finish:", method, fmt.Sprint(reply), e)
    }

    /*
        这是一个查看商品列表的 Action,cmd 是入参，result 是出参，在 protocol中定义，下文详细介绍。
    */
    func (self *ProductController) Store(cmd business_cmd.Store, result *business_value.Store) (e error) {
        var (
            //创建一个 service包里的  Product 对象指针
            productSer *service.Product
        )
        *result = business_value.Store{}

        //通过 父类 Service 方法获取 service.Product 类型的服务对象。
        //因为 go 没有泛型，实现服务定位器模式，只可依赖二级指针，不用管原理，直接取。
        self.Service(&productSer)

        //使用服务的 Store 方法 读取出商品数据， 并且赋值给出参 result 
        result.List, e = productSer.Store()
        return
    }
```

### business_service
```go
    /* 
         learning/business/service/product.go
    */
    func init() {
        // RegisterService 注册 service 与控制器 self.Service(&) 关联
        business.RegisterService(new(Product).Gotree())
    }

    type Product struct {
        //继承 BusinessService 基类
        business.BusinessService
    }

    // gotree 风格构造
    func (self *Product) Gotree() *Product {
        self.BusinessService.Gotree(self)
        return self
    }

    // 读取商品服务 返回一个商品信息匿名结构体数组。
    func (self *Product) Store() (result []struct {
        Id    int64 //商品 id
        Price int64 //商品价格
        Desc  string //商品描述
    }, e error) {

        //创建 dao调用命令
        cmdPt := new(dao_cmd.ProductGetList).Gotree([]int64{1, 2})
        //创建 dao返回数据
        store := dao_value.ProductGetList{}

        //CallDao 调用 Dao 服务器的 Com 入参cmdPt 出参store
        e = self.CallDao(cmdPt, &store)
        if e == helper.ErrBreaker {
            //熔断处理
            helper.Log().Notice("Store ErrBreaker")
            return
        }
        result = store.List
        return
    }
```

### business_cmd
```go
    /* 
        learning/protocol/business_cmd/product.go
    */
    func init() {
        //Store 加入熔断 条件:15秒内 %50超时, 60秒后恢复
        rc.RegisterBreaker(new(Store), 15, 0.5, 60)
    }

    // 定义访问 business product 控制器的命令基类， 所有的 business.product 动作调用，继承于这个基类
    type productCmdBase struct {
        rc.RpcCmd //所有远程调用的基类
    }

    // Gotree 风格构造，因为是基类，参数需要暴露 child
    func (self *productCmdBase) Gotree(child ...interface{}) *productCmdBase {
        self.RpcCmd.Gotree(self)
        self.AddChild(self, child...)
        // self.AddChild 继承原型链, 用于以后实现多态。
        return self
    }

    // 多态方法重写 Control。用于定位该命令，要访问的控制器。 这里填写 "Product" 控制器
    func (self *productCmdBase) Control() string {
        return "Product"
    }


    // 定义一个 product 的动作调用
    type Store struct {
        productCmdBase  //继承productCmdBase
        Ids            []int64
        TestEmpty     int `opt:"empty"` //如果值为 []、""、0,加入此 tag ,否则会报错!
    }

    func (self *Store) Gotree(ids []int64) *Store {
        //调用父类 productCmdBase.Gotree 传入自己的对象指针
        self.productCmdBase.Gotree(self)
        self.Ids = ids
        return self
    }

    // 多态方法 重写 Action。用于定位该命令，要访问控制器里的 Action。 这里填写 "Store" 动作
    func (self *Store) Action() string {
        return "Store"
    }
```

### dao_cmd
```go
    /* 
        learning/protocol/dao_cmd/product.go
    */

    // 定义访问 dao product 控制器的命令基类， 所有的 dao.product 动作调用，继承于这个基类
    type productCmdBase struct {
        rc.RpcCmd
    }

    func (self *productCmdBase) Gotree(child ...interface{}) *productCmdBase {
        self.RpcCmd.Gotree(self)
        self.AddChild(self, child...)
        return self
    }

    // 上文已介绍
    func (self *productCmdBase) Control() string {
        return "Product"
    }

    // 多态方法重写 ComAddr 用于多 Dao节点 时的分布规则，当前返回随机节点
    func (self *productCmdBase) ComAddr(rn rc.ComNode) string {
        //分布于com.conf配置相关
        //rn.RandomAddr() 随机节点访问
        //rn.BalanceAddr() 负载均衡节点访问
        //rn.DummyHashAddr(self.productId) 一致性哈希节点访问
        //rn.AllNode() 获取全部节点,自定义方式访问
        //rn.SlaveAddr()  //返回随机从节点  主节点:节点id=1,当只有主节点返回主节点
        //rn.MasterAddr() //返回主节点 主节点:节点id=1
        return rn.RandomAddr()
    }

    // 定义一个 ProductGetList 的动作调用
    type ProductGetList struct {
        productCmdBase //继承productCmdBase
        Ids            []int64
    }

    func (self *ProductGetList) Gotree(ids []int64) *ProductGetList {
        self.productCmdBase.Gotree(self)
        self.Ids = ids
        return self
    }

    // 多态方法 重写 Action。
    func (self *ProductGetList) Action() string {
        return "GetList"
    }
```

### com_controller
```go
    /* 
         learning/dao/controllers/product_controller.go
         Dao 组件入口控制器， 关联dao_cmd
    */
    func init() {
        // 注册 Product 数据控制器入口
        dao.RegisterController(new(ProductController).Gotree())
    }

    // 定义Com控制器，控制器对象命名 `Name`Controller，`Name` 等同 Com， 继承控制器基类 dao.ComController
    type ProductController struct {
        dao.ComController
    }

    // Gotree
    func (self *ProductController) Gotree() *ProductController {
        self.ComController.Gotree(self)
        return self
    }

    // 实现动作 GetList
    func (self *ProductController) GetList(cmd dao_cmd.ProductGetList, result *dao_value.ProductGetList) (e error) {
        var (
            //创建一个 sources.models 包里的 Product 对象指针, sources.models : 数据库表模型
            mProduct *product.Product
        )
        *result = dao_value.ProductGetList{}
        // 服务定位器获取 product.Product 实例
        self.Model(&mProduct)
        // 取数据库数据赋值给出参 result.List
        result.List, e = mProduct.Gets(cmd.Ids)
        return
    }

    // 实现动作 Add， 事务示例
    func (self *ProductController) Add(cmd dao_cmd.ProductAdd, result *helper.VoidValue) (e error) {
        var (
            mProduct *product.Product
        )
        *result = helper.VoidValue{}
        self.Model(&mProduct)

        // Transaction 执行事务，如果返回 不为 nil，触发回滚。 
        self.Transaction(func() error {
           _, e := mProduct.Add(cmd.Desc, cmd.Price)
           if e != nil {
               return
           }
           _, e = mProduct.Add(cmd.Desc, cmd.Price)
           return e
        })

        return
    }
```

### com_model
```go
    /* 
         learning/dao/sources/models/product/product.go
         数据库表模型示例，与 db 配置文件 Com 相关, learning/dao/conf/dev/db.conf   
    */
    func init() {
        //注册 Product 模型
        dao.RegisterModel(new(Product).Gotree())
    }

    // 定义一个模型 Product 继承模型基类 ComModel
    type Product struct {
        dao.ComModel
    }

    // Gotree
    func (self *Product) Gotree() *Product {
        self.ComModel.ComModel(self)
        return self
    }

    //多态方法 重写 主要用于绑定 Com, com.conf 统一控制组件开启
    func (self *Product) Com() string {
        return "Product"
    }

    // Gets
    func (self *Product) Gets(productId []int64) (list []struct {
        Id    int64
        Price int64
        Desc  string
    }, e error) {
        /*
            FormatPlaceholder()  :处理转数组为 ?,?,?
            FormatArray() : 处理数组为 value,value,value
            self.Conn().Raw() : 获取连接执行sql语句
            QueryRows() : 获取多行数据
        */
        sql := fmt.Sprintf("SELECT id,price,`desc` FROM `product` where id in(%s)", self.FormatPlaceholder(productId))
        _, e = self.Conn().Raw(sql, self.FormatArray(productId)...).QueryRows(&list)
        return
    }
```

## 高级教程
### 进阶使用
```sh
$ vi $GOPATH/src/learning/dao/conf/dev/cache.conf
# 编辑 redis 配置，Com = Feature、 服务器地址 = 127.0.0.1、端口 = 6379 密码 = 、db = 0
# Feature = "server=127.0.0.1:6379;password=;database=0"

$ vi $GOPATH/src/learning/dao/conf/dev/com.conf
# 开启 Feature = 1，1代表组件ID, 如果要负载多台dao，在其他机器递增ID

$ vi $GOPATH/src/learning/business/conf/dev/business.conf
# 开启 TimerOn = "Feature"

$ cd $GOPATH/src/learning/dao
$ go run main.go
$ cd $GOPATH/src/learning/business
$ go run main.go

# 观察日志和查阅相关 Feature 代码
```

### timer
```go
    /* 
         learning/business/timer/feature.go
         定时器示例 learning/business/conf/dev/business.conf -> TimerOn，控制定期的开启和关闭
    */
    func init() {
        // RegisterTimer 注册定时器
        business.RegisterTimer(new(Feature).Gotree())
    }

    // Feature
    type Feature struct {
        business.BusinessTimer
    }

    // Feature
    func (self *Feature) Gotree() *Feature {
        self.BusinessTimer.Gotree(self)
        //注册触发定时器， 每5000毫秒秩序
        self.RegisterTickTrigger(5000, self.CourseTick)

        //注册每日定时器，每日3点1分执行
        self.RegisterDayTrigger(3, 1, self.CourseDay)
        return self
    }

    // CourseTick
    func (self *Feature) CourseTick() {
        var (
            //learning/business/service/feature.go
            featureSer *service.Feature
        )
        //服务定位器获取 Feature 服务，  
        self.Service(&featureSer)

        //异步调用Feature.Course方法
        self.Async(func(ac business.AsyncController) {
            featureSer.Course()
        })

        /*
            1. 全局禁止使用go func(), 请使用Async。
            2. 底层做了优雅关闭和热更新, hook了 async。 保证会话请求的闭环执行, 防止造成脏数据。
            3. async 做了 recover 和 栈传递，可以有效的使用同gseq
        */
    }
```

### helper
```go
    /* 
         learning/business/service/feature.go
         展示 Helper 的使用， 包含了一些辅助函数。
    */
    func (self *Feature) Simple() (result []struct {
        Id    int
        Value string
        Pos   float64
    }, e error) {
        var mapFeature map[int]struct {
            Id    int
            Value string
        }
        //使用 NewMap 函数，创建匿名结构体的 map
        helper.NewMap(&mapFeature)

        var newFeatures []struct {
            Id    int
            Value string
        }
        //使用 NewSlice 函数，创建匿名结构体的数组
        if e = helper.NewSlice(&newFeatures, 2); e != nil {
            return
        }
        for index := 0; index < len(newFeatures); index++ {
            newFeatures[index].Id = index + 1
            newFeatures[index].Value = "hello"

            //匿名数组结构体赋值赋值给 匿名map结构体
            mapFeature[index] = newFeatures[index]
        }

        //内存拷贝，支持数组，结构体。
        if e = helper.Memcpy(&result, newFeatures); e != nil {
            return
        }

        //反射升序排序
        helper.SliceSortReverse(&result, "Id")
        //反射降序排序
        helper.SliceSort(&result, "Id")

        //group go并发
        group := helper.NewGroup()
        group.Add(func() error {
            //配置文件读取 域名::key名
            mode := helper.Config().String("sys::Mode")
            helper.Log().Notice("Notice", mode)
            return nil
        })
        group.Add(func() error {
            //配置文件读取 域名::key名
            len := helper.Config().DefaultInt("sys::LogWarnQueueLen", 512)
            helper.Log().Warning("Warning", len)
            return nil
        })
        group.Add(func() error {
            helper.Log().Debug("Debug")
            return nil
        })

        //等待以上3个并发结束
        group.Wait()
        return
    }
```

### cache
```go
    /* 
        代码文件  learning/dao/sources/cache/course.go
        配置文件  learning/dao/conf/dev/cache.conf
        展示 redis 缓存数据源的使用
    */
    func init() {
        dao.RegisterCache(new(Course).Gotree())
    }

    // Course
    type CourseCache struct {
        dao.ComCache            // 继承缓存基类
    }

    // Course
    func (self *Course) Gotree() *Course {
        self.ComCache.Gotree(self)
        return self
    }

    // 多态方法 重写 主要用于绑定 Com, com.conf 统一控制组件开启
    func (self *Course) Com() string {
        return "Feature"
    }

    func (self *Course) TestGet() (result struct {
        CourseInt    int
        CourseString string
    }, err error) {

        // self.do 函数，调用redis
        strData, err := redis.Bytes(self.Do("GET", "Feature"))
        if err != nil {
            return
        }
        err = json.Unmarshal(strData, &result)
        return
    }    
```

### memory
```go
    /* 
        代码文件  learning/dao/sources/memory/course.go
        展示内存数据源的使用
    */
    func init() {
        dao.RegisterMemory(new(Course).Gotree())
    }

    // Course
    type Course struct {
        dao.ComMemory    //继承内存基类
    }

    // Gotree
    func (self *Course) Gotree() *Course {
        self.ComMemory.Gotree(self)
        return self
    }

    // 多态方法 重写 主要用于绑定 Com, com.conf 统一控制组件开启
    func (self *Course) Com() string {
        return "Feature"
    }

    func (self *Course) TestSet(i int, s string) {
        var data struct {
            CourseInt    int
            CourseString string
        }
        data.CourseInt = i
        data.CourseString = s
        if self.Setnx("Feature", data) {
            //如果 "Feature" 不存在
            self.Expire("Feature", 5)   //Expire 设置生存时间
        }
        self.Set("Feature", data) //直接覆盖

        //Get 存在返回true, 不存在反回false
        exists := self.Get("Feature", &data)
    }
```

### api
```go
    /* 
        代码文件  learning/dao/sources/api/tao_bao_ip.go
        配置文件  learning/dao/conf/api.conf
        展示 http 数据源的使用
    */
    func init() {
        dao.RegisterApi(new(TaoBaoIp).Gotree())
    }

    // TaoBaoIp
    type TaoBaoIp struct {
        dao.ComApi
    }

    //Gotree
    func (self *TaoBaoIp) Gotree() *TaoBaoIp {
        self.ComApi.Gotree(self)
        return self
    }

    // 绑定配置文件[api]域下的host地址
    func (self *TaoBaoIp) Api() string {
        return "TaoBaoIp"
    }

    // GetIpInfo
    func (self *TaoBaoIp) GetIpInfo(ip string) (country string, err error) {
        //doc http://ip.taobao.com/instructions.html
        
        //get post postjson
        data, err := self.HttpGet("/service/getIpInfo.php", map[string]interface{}{"ip": ip})
        //data, err := self.HttpPost("/service/getIpInfo.php", map[string]interface{}{"ip": ip})
        //data, err := self.HttpPostJson("/service/getIpInfo.php", map[string]interface{}{"ip": ip})
    }
```

### unit
```go
    /* 
        business 单元测试
        代码目录  learning/business/unit
        测试service对象，请在本机开启dao 进程。 TestOn : "Com组件名字:id"
        TestOn 函数内部有引用框架，初始化、建立连接等。填写Com 即可使用。
        执行命令 go test -v -count=1 -run TestProduct $GOPATH/src/learning/business/unit/service_test.go
    */
    func TestProduct(t *testing.T) {
        service := new(service.Product).Gotree()
        //开启单元测试 填写 com
        service.TestOn("Product:1", "User:1", "Order:1")
        
        t.Log(service.Store())
        t.Log(service.Shopping(1, 1))
    }

    /*
        dao 单元测试
        代码目录  learning/dao/unit
        TestOn 函数内部有引用框架，初始化、建立 redis、mysql 连接等。
        执行命令 go test -v -count=1 -run TestFeature $GOPATH/src/learning/dao/unit/feature_test.go
    */
    func TestFeature(t *testing.T) {
        // 四种数据源对象的单元测试
        api := new(api.TaoBaoIp).Gotree()
        cache := new(cache.Course).Gotree()
        memory := new(memory.Course).Gotree()
        model := new(product.Product).Gotree()

        //开启单元测试
        api.TestOn()
        cache.TestOn()
        memory.TestOn()
        model.TestOn()

        t.Log(api.GetIpInfo("49.87.27.95"))
        t.Log(cache.TestGet())
        t.Log(memory.TestGet())
        t.Log(model.Gets([]int64{1, 2, 3, 4}))
    }
```

### command
###### ./dao telnet        该命令尝试连接数据库、redis。用来检验防火墙和密码。 
###### ./dao start         该命令会以督程的方式启动 dao。
###### ./dao stop          该命令以优雅关闭的方式停止 dao, 会等待 dao 执行完当前未完成的请求。
###### ./dao restart       该命令以热更新的方式重启 dao。
###### ./business start    该命令会以督程的方式启动 business。
###### ./business stop     该命令以优雅关闭的方式停止 business, 会等待 business 执行完当前未完成的请求。
###### ./business restart  该命令以热更新的方式重启 business。
###### ./business qps      该命令查看当前 business 调用 dao 的 qps 信息， -t 实时刷新。
###### ./business status   该命令查看当前 business 状态信息
```sh
    $ cd $GOPATH/src/learning/dao
    $ go build
    $ ./dao start
    $ cd $GOPATH/src/learning/business
    $ go build
    $ ./business start
    
    #执行一个单元测试
    $ go test -v -count=1 -run TestUserRegister $GOPATH/src/learning/business/unit/gateway_test.go
    
    #查看qps，实时加 -t ./business qps -t
    $ ./business qps
    
    #查看状态
    $ ./business status

    #关闭
    $ ./business stop
    $ cd $GOPATH/src/learning/dao
    $ ./dao stop
```

### dispersed
```sh
    # dao 实例 1
    $ cd $GOPATH/src/learning/dao
    $ go build
    $ vi $GOPATH/src/learning/dao/conf/dev/dispersed.conf
    # 修改为 BusinessAddrs = "127.0.0.1:8888,127.0.0.1:18888"
    $ ./dao start #启动 dao 实例1

    # dao 实例 2 
    $ vi $GOPATH/src/learning/dao/conf/dev/dispersed.conf
    # 修改为
    # BindAddr = "127.0.0.1:16666"
    $ vi $GOPATH/src/learning/dao/conf/dev/com.conf
    # 修改为
    # Order = 2
    # User = 2
    # Product = 2
    $ ./dao start #启动 dao 实例2

    # business 实例 1
    $ cd $GOPATH/src/learning/business
    $ go build
    $ ./business start

    # business 实例 2
    $ vi $GOPATH/src/learning/business/conf/dev/dispersed.conf
    # 修改为
    # BindAddr = "0.0.0.0:18888"
    $ ./business start
    $ ps


    # 单元测试 多实例
    $ vi $GOPATH/src/learning/business/unit/gateway_test.go
    # 加入新实例
    # gateway.AppendBusiness("127.0.0.1:8888")
    # gateway.AppendBusiness("127.0.0.1:18888")
    $ go test -v -count=1 -run TestStore $GOPATH/src/learning/business/unit/gateway_test.go
```