<?php

namespace yzh52521\Task\util;

class MacAddress
{
    private $return_array = []; // 返回带有MAC地址的字串数组

    private $mac;

    public function Local_Mac_Address()
    {
        switch (strtolower(PHP_OS)) {
            case "darwin":
            case "linux" :
                $this->forLinux();
                break;
            case "unix":
            case "aix":
            case "solaris" :
                break;
            default :
                $this->forWindows();
                break;
        }
        $temp_array = [];
        foreach ($this->return_array as $value) {
            if (preg_match("/[0-9a-f][0-9a-f][:-]" . "[0-9a-f][0-9a-f][:-]" . "[0-9a-f][0-9a-f][:-]" . "[0-9a-f][0-9a-f][:-]" . "[0-9a-f][0-9a-f][:-]" . "[0-9a-f][0-9a-f]/i", $value, $temp_array)) {
                $this->mac = $temp_array [0];
                break;
            }
        }
        unset ($temp_array);
        return $this->mac;
    }

    private function forWindows()
    {
        @exec("ipconfig /all", $this->return_array);
        if ($this->return_array)
            return $this->return_array;
        else {
            $ipconfig = $_SERVER["WINDIR"] . "\system32\ipconfig.exe";
            if (is_file($ipconfig))
                @exec($ipconfig . " /all", $this->return_array);
            else
                @exec($_SERVER["WINDIR"] . "\system\ipconfig.exe /all", $this->return_array);
            return $this->return_array;
        }
    }


    private function forLinux()
    {
        @exec("ifconfig -a", $this->return_array);
        return $this->return_array;
    }
}