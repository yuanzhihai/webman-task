<?php

use yzh52521\Task\Server;

return [
    'cron_task' => [
        'handler' => Server::class,
        'listen'  => 'text://' . config('plugin.yzh52521.task.app.task.listen'), // 这里用了text协议，也可以用frame或其它协议
        'count'   => 1, // 必须是1
    ]
];