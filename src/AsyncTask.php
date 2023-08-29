<?php

declare(strict_types=1);
/**
 * This file is part of Heros-Worker.
 *
 * @contact  chenzf@pvc123.com
 */

namespace yzh52521\Task;

use support\Container;
use support\Log;
use Workerman\Connection\TcpConnection;

/**
 * 异步worker.
 */
class AsyncTask
{
    public function onMessage(TcpConnection $connection, string $data): void
    {
        $class = json_decode($data, true);
        if (class_exists($class['class']) && method_exists($class['class'], $class['method'])) {
            try {
                $code       = 0;
                $instance   = Container::get($class['class']);
                $parameters = $class['parameter'] ?? [];
                $res        = call_user_func([$instance, $class['method']], $parameters);
            } catch (\Throwable $throwable) {
                $code = 1;
                $res  = $throwable->getMessage();
            }
        } else {
            $code = 1;
            $res  = "方法或类不存在或者错误";
        }
        $connection->send(json_encode(['code' => $code, 'msg' => $res]));
    }
}
