<?php
namespace Hdfs;


/**
* Interface for HDFS responses
*
*/
interface IResponse  {

    /**
    * Check response for errors and build exceptions depending on errors types
    *
    * @return null|\Hdfs\Exception
    */
    public function getException () ;

}//interface IResponse