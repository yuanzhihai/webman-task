<?php
return [
    'enable' => true,
    'task'   => [
        'listen'            => '0.0.0.0:2345',
        'async_listen'      => '0.0.0.0:2346',
        'crontab_table'     => 'system_crontab', //任务计划表
        'crontab_table_log' => 'system_crontab_log',//任务计划流水表
        'prefix'            => 'th_', //表前缀 与 database 设置一致
        'debug'             => true, //控制台输出日志
        'write_log'         => true,// 任务计划日志
        'runInBackground'   => false //命令行任务是否后台运行
    ],
];
