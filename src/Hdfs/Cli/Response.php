<?php
namespace Hdfs\Cli;


/**
* Object represents result of executing `hadoop fs` commands
*
*/
class Response implements \Hdfs\IResponse
{
    /** @var string Command which produced this response */
    public $command = '';
    /** @var int Command execution exit code */
    public $exitCode = 0;
    /** @var string STDOUT of executed command */
    public $stdout = '';
    /** @var string STDERR of executed command */
    public $stderr = '';
    /** @var \Hdfs\Exception Cached exception instance */
    protected $exception = null;


    /**
    * Check if command failed to execute
    *
    */
    public function hasError ()
    {
        return (bool)$this->stderr;
    }//function hasError()


    /**
    * Get exception depending on contents of $this->stderr
    *
    * @return null|\Hdfs\Exception
    */
    public function getException ()
    {
        //exception instance already created
        if ($this->exception)
        {
            return $this->exception;
        }

        //no errors
        if (!$this->hasError())
        {
            return null;
        }
        $this->exception = null;

        //just a notice
        if (
               stripos($this->stderr, ' NOTICE ') !== false
            || stripos($this->stderr, ' INFO ') !== false
        )
        {
            return null;
        }

        if (stripos($this->stderr, 'permission denied') !== false)
        {
            $this->exception = new \Hdfs\Exception\PermissionException($this->stderr, false);
        }
        elseif (stripos($this->stderr, 'file exists') !== false)
        {
            $this->exception = new \Hdfs\Exception\AlreadyExistsException($this->stderr, false);
        }
        elseif (stripos($this->stderr, 'no such file or directory') !== false)
        {
            $this->exception = new \Hdfs\Exception\NotFoundException($this->stderr, false);
        }
        elseif (stripos($this->stderr, 'directory is not empty') !== false)
        {
            $this->exception = new \Hdfs\Exception\NotEmptyException($this->stderr, false);
        }
        elseif (stripos($this->stderr, 'is not a directory') !== false)
        {
            $this->exception = new \Hdfs\Exception\IllegalArgumentException($this->stderr, false);
        }
        elseif (stripos($this->stderr, 'non-super user cannot') !== false)
        {
            $this->exception = new \Hdfs\Exception\PermissionException($this->stderr, false);
        }

        if (!$this->exception)
        {
            $this->exception = new \Hdfs\Exception($this->stderr, false);
        }

        return $this->exception;
    }//function getException()


}//class Response