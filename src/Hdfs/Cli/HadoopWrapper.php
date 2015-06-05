<?php
namespace Hdfs\Cli;



/**
* Wrapper for `hadoop fs` shell command
*
*/
class HadoopWrapper
{
    /** @var \Closure Callback which accepts one argument: \Hdfs\Cli\Response */
    protected $debugCallback = null;

    /**
    * Parse output of `hadoop -ls /somepath` command
    *
    * @param string $output
    * @param bool $isAssoc
    * @param string $relativeToPath
    *
    * @return array
    */
    static public function parseLsOutput ($output, $isAssoc, $relativeToPath = '')
    {
        $list = array();

        $reType    = '([-a-z])';
        $rePerm    = '([-rwxsStT]{9})';
        $reRfactor = '([-0-9]+)';
        $reOwner   = '([^\s]+)';
        $reGroup   = '([^\s]+)';
        $reSize    = '(\d+)';
        $reMtime   = '(\d{4})-(\d{2})-(\d{2})\s+(\d{2}):(\d{2})';

        $regexp = "/^$reType$rePerm\s+$reRfactor\s+$reOwner\s+$reGroup\s+$reSize\s+$reMtime\s+/";

        $lines  = explode("\n", $output);
        $cLines = count($lines);

        for ($idx = 0; $idx < $cLines; $idx ++)
        {
            $match = array();
            if (preg_match($regexp, $lines[$idx], $match))
            {
                $name = str_replace($match[0] . $relativeToPath, '', $lines[$idx]);
                if (strpos($name, '/') === 0)
                {
                    $name = substr($name, 1);
                }

                $node = array(
                    'name'    => $name,
                    'type'    => ($match[1] == 'd' ? \Hdfs\EntryType::DIRECTORY : \Hdfs\EntryType::FILE),
                    'mode'    => self::extractPermissions($match[2]),
                    'rfactor' => intval($match[3]),
                    'owner'   => $match[4],
                    'group'   => $match[5],
                    'size'    => doubleval($match[6]),
                    'mtime'   => mktime($match[10], $match[11], 0, $match[8], $match[9], $match[7])
                );
                $list[] = ($isAssoc ? $node : new \Hdfs\EntryStatus($node));
            }
        }

        return $list;
    }//function parseLsOutput()


    /**
    * Get decimal permissions value based on a permissions line acquired from `readDir` or `stat` output
    *
    * @param string $perm
    *
    * @return int
    */
    static protected function extractPermissions ($perm)
    {
        $perms = str_split($perm);

        $mode = 0;

        if ($perms[0] == 'r') $mode += 0400;
        if ($perms[1] == 'w') $mode += 0200;
        if ($perms[2] == 'x') $mode += 0100;
        else if ($perms[2] == 's') $mode += 04100;
        else if ($perms[2] == 'S') $mode += 04000;

        if ($perms[3] == 'r') $mode += 040;
        if ($perms[4] == 'w') $mode += 020;
        if ($perms[5] == 'x') $mode += 010;
        else if ($perms[5] == 's') $mode += 02010;
        else if ($perms[5] == 'S') $mode += 02000;

        if ($perms[6] == 'r') $mode += 04;
        if ($perms[7] == 'w') $mode += 02;
        if ($perms[8] == 'x') $mode += 01;
        else if ($perms[8] == 't') $mode += 01001;
        else if ($perms[8] == 'T') $mode += 01000;

        return $mode;
    }//function extractPermissions()


    /**
    * Constructor
    *
    */
    public function __construct ()
    {

    }//function __construct()


    /**
    * Set callback wich will be invoked on each CLI request.
    * Should accept one arguments:
    *   - \Hdfs\Cli\Response - Result of CLI command execution.
    *
    * @param Closure $callback
    */
    public function setDebugCallback (\Closure $callback)
    {
        $this->debugCallback = $callback;
    }//function setDebugCallback()


    /**
    * Execute command via `hadoop fs`
    *
    * @param string ... List of arguments for `hadoop fs` command
    *
    * @return \Hdfs\Cli\Response
    */
    public function exec ()
    {
        $result = new Response();

        $args = array();
        foreach (func_get_args() as $arg)
        {
            $args[] = escapeshellarg($arg);
        }
        $result->command = "hadoop fs ". implode(' ', $args);

        $pipes = array();
        $p = proc_open(
            $result->command,
            array(0 => array("pipe", "r"), 1 => array("pipe", "w"), 2 => array("pipe", "w")),
            $pipes
        );

        $result->stdout   = stream_get_contents($pipes[1]);
        $result->stderr   = stream_get_contents($pipes[2]);
        $result->exitCode = proc_close($p);

        if (!is_null($this->debugCallback))
        {
            $fn = $this->debugCallback;
            $fn(clone $result);
        }

        return $result;
    }//function exec()

}//class HadoopWrapper