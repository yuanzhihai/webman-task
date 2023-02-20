# 动态秒级定时任务

## 概述

基于 **webman** + **Laravel-ORM** 的动态秒级定时任务管理，兼容 Windows 和 Linux 系统。

使用`tp-orm`

```shell
composer require yzh52521/webman-task
```
使用`laravel orm`
```shell
composer require yzh52521/webman-task dev-lv
```

## 简单使用

```
   $param = [
     'method' => 'crontabIndex',//计划任务列表
     'args'   => ['limit' => 10, 'page' => 1]//参数
    ];
   $result= yzh52521\Task\Client::instance()->request($param);
   return json($result);

```

## 计划任务列表

### 方法名

**method：** crontabIndex

### 请求参数

**args**

| 参数名称 | 是否必须 | 示例                     | 备注         |
| -------- | -------- | ------------------------ | ------------ |
| page     | 是       | 1                        | 页码         |
| limit    | 是       | 15                       | 每页条数     |

### 返回数据

```json
{
"code": 200,
"msg": "ok",
"data": {
"total": 4,
"per_page": 15,
"current_page": 1,
"last_page": 1,
"data": [
{
"id": 6,
"title": "class任务 每月1号清理所有日志",
"type": 2,
"rule": "0 0 1 * *",
"target": "app\\common\\crontab\\ClearLogCrontab",
"parameter": "",
"running_times": 71,
"last_running_time": 1651121710,
"remark": "",
"sort": 0,
"status": 1,
"create_time": 1651114277,
"update_time": 1651114277,
"singleton": 1
},
{
"id": 5,
"title": "eavl任务 输出 hello world",
"type": 4,
"rule": "* * * * *",
"target": "echo 'hello world';",
"parameter": "",
"running_times": 25,
"last_running_time": 1651121701,
"remark": "",
"sort": 0,
"status": 1,
"create_time": 1651113561,
"update_time": 1651113561,
"singleton": 0
},
{
"id": 3,
"title": "url任务 打开 workerman 网站",
"type": 3,
"rule": "*/20 * * * * *",
"target": "https://www.workerman.net/",
"parameter": "",
"running_times": 39,
"last_running_time": 1651121700,
"remark": "请求workerman网站",
"sort": 0,
"status": 1,
"create_time": 1651112925,
"update_time": 1651112925,
"singleton": 0
},
{
"id": 1,
"title": "command任务 输出 webman 版本",
"type": 1,
"rule": "*/20 * * * * *",
"target": "version",
"parameter": null,
"running_times": 112,
"last_running_time": 1651121700,
"remark": "20秒",
"sort": 0,
"status": 1,
"create_time": 1651047480,
"update_time": 1651047480,
"singleton": 1
}
]
}
}
```

## 计划任务日志列表

**method：** crontabLog

### 请求参数

**args**

| 参数名称       | 是否必须 | 示例        | 备注         |
|------------| -------- | ----------- | ------------ |
| page       | 是       | 1           | 页码         |
| limit      | 是       | 15          | 每页条数     |
| crontab_id | 否       | 1           | 计划任务ID   |

### 返回数据

```json

{
  "code": 200,
  "msg": "ok",
  "data": {
    "total": 97,
    "per_page": 15,
    "current_page": 1,
    "last_page": 7,
    "data": [
      {
        "id": 257,
        "crontab_id": 1,
        "target": "version",
        "parameter": "",
        "exception": "Webman-framework v1.3.11",
        "return_code": 0,
        "running_time": "0.834571",
        "create_time": 1651123800,
        "update_time": 1651123800
      },
      {
        "id": 251,
        "crontab_id": 1,
        "target": "php webman version",
        "parameter": "",
        "exception": "Webman-framework v1.3.11",
        "return_code": 0,
        "running_time": "0.540384",
        "create_time": 1651121700,
        "update_time": 1651121700
      },
      {
        "id": 246,
        "crontab_id": 1,
        "target": "php webman version",
        "parameter": "{}",
        "exception": "Webman-framework v1.3.11",
        "return_code": 0,
        "running_time": "0.316019",
        "create_time": 1651121640,
        "update_time": 1651121640
      },
      {
        "id": 244,
        "crontab_id": 1,
        "target": "php webman version",
        "parameter": "{}",
        "exception": "Webman-framework v1.3.11",
        "return_code": 0,
        "running_time": "0.493848",
        "create_time": 1651121580,
        "update_time": 1651121580
      }
    ]
  }
}

```

## 添加任务

**method：** crontabCreate

### 请求参数

**args**

| 参数名称   | 参数类型 | 是否必须 | 示例                          | 备注                                                |
|--------| -------- |-----|-----------------------------|---------------------------------------------------|
| title  | text     | 是   | 输出 webman 版本                | 任务标题                                              |
| type   | text     | 是   | 1                           | 任务类型 (1 command, 2 class, 3 url, 4 eval ,5 shell) |
| rule   | text     | 是   | */3 * * * * *               | 任务执行表达式                                           |
| target | text     | 是   | php webman version/ version | 调用任务字符串                                           |
| parameter | text     | 否   | {}                          | 调用任务参数(url和eval无效)                                |
| remark | text     | 是   | 每3秒执行                       | 备注                                                |
| sort   | text     | 是   | 0                           | 排序                                                |
| status | text     | 是   | 1                           | 状态[0禁用; 1启用]                                      |
| singleton | text     | 否    | 1                           | 是否单次执行 [0 是 1 不是]                                 |


### 返回数据

```json
{
  "code": 200,
  "msg": "ok",
  "data": {
    
  }
}
```

## 重启任务

**method：** crontabReload

### 请求参数

**args**

| 参数名称 | 参数类型 | 是否必须 | 示例 | 备注 |
| -------- | -------- | -------- | ---- | ---- |
| id       | text     | 是       | 1,2  |   计划任务ID 多个逗号隔开   |

### 返回数据

```json
{
  "code": 200,
  "msg": "ok",
  "data": {
    
  }
}
```
## 修改任务

**method：** crontabUpdate

### 请求参数

**args**

| 参数名称   | 参数类型 | 是否必须 | 示例                         | 备注                                               |
|--------| -------- |------|----------------------------|--------------------------------------------------|
| id     | text     | 是    | 1                          |                                                  |
| title  | text     | 否    | 输出 webman 版本               | 任务标题                                             |
| type   | text     | 否    | 1                          | 任务类型 (1 command, 2 class, 3 url, 4 eval，5 shell) |
| rule   | text     | 否    | */3 * * * * *              | 任务执行表达式                                          |
| target | text     | 否    | php webman version/version | 调用任务字符串                                          |
| parameter | text     | 否    | {}                         | 调用任务参数(url和eval无效)                               |
| remark | text     | 否    | 每3秒执行                      | 备注                                               |
| sort   | text     | 否    | 0                          | 排序                                               |
| status | text     | 否    | 1                          | 状态[0禁用; 1启用]                                     |
| singleton | text     | 否    | 1                          | 是否单次执行 [0 是 1 不是]                                |

### 返回数据

```json

{
  "code": 200,
  "msg": "ok",
  "data": {
    
  }
}
```

## 删除任务

**method：** crontabDelete

### 请求参数

**args**

| 参数名称 | 参数类型 | 是否必须 | 示例 | 备注 |
| -------- | -------- | -------- | ---- | ---- |
| id       | text     | 是       | 1,2  |  计划任务ID 多个逗号隔开    |

### 返回数据

```
{
  "code": 200,
  "msg": "ok",
  "data": {
    
  }
}
```


