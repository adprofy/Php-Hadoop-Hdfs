<?php

require_once __DIR__.'/../../BaseCase.php';


/**
* \Hdfs\Web\Response
*
*/
class WebResponseTest extends BaseCase
{

    /**
    * Check AlreadyExistsException is being built correctly
    *
    */
    public function testAlreadyExists ()
    {
        $response = $this->getWebResponse('already_exists');

        $exception = $response->getException();

        $success = ($exception instanceof \Hdfs\Exception\AlreadyExistsException);
        $this->assertTrue($success);
    }//function testAlreadyExists()


    /**
    * Check PermissionException is being built correctly
    *
    */
    public function testPermission ()
    {
        $response = $this->getWebResponse('permission_denied');

        $exception = $response->getException();

        $success = ($exception instanceof \Hdfs\Exception\PermissionException);
        $this->assertTrue($success);
    }//function testPermission()


    /**
    * Check NotFoundException is being built correctly
    *
    */
    public function testNotFound ()
    {
        $response = $this->getWebResponse('not_found');

        $exception = $response->getException();

        $success = ($exception instanceof \Hdfs\Exception\NotFoundException);
        $this->assertTrue($success);
    }//function testNotFound()


    /**
    * Check NotEmptyException is being built correctly
    *
    */
    public function testNotEmpty ()
    {
        $response = $this->getWebResponse('not_empty');

        $exception = $response->getException();

        $success = ($exception instanceof \Hdfs\Exception\NotEmptyException);
        $this->assertTrue($success);
    }//function testNotEmpty()

}//class WebResponseTest