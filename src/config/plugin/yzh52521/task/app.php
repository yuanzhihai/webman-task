<?php
return [
    'enable' => true,
    'task'   => [
        'listen'            => '0.0.0.0:2345',
        'crontab_table'     => 'system_crontab', //任务计划表
        'crontab_table_log' => 'system_crontab_log',//任务计划流水表
        'debug'             => true,
    ],
];
