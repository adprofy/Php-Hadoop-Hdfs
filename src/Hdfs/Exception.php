<?php
namespace Hdfs;



/**
* Base class for HDFS-related exceptions
*
*/
class Exception extends \Exception
{
    /**
    * If this exception happened with local files
    * @var bool
    */
    protected $local = false;


    /**
    * Constructor
    *
    * @param string $message
    * @param bool $isLocal      whether this exception happened with local files or remote files
    */
    public function __construct ($message, $isLocal)
    {
        $this->local = (bool)$isLocal;
        parent::__construct($message);
    }//function __construct()


    /**
    * Identifies if this exception happened with local files
    *
    * @return bool
    */
    public function isLocal ()
    {
        return $this->local;
    }//function isLocal()


}//class Exception