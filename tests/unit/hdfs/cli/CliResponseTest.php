<?php

require_once __DIR__.'/../../BaseCase.php';


/**
* \Hdfs\Cli\Response
*
*/
class CliResponseTest extends BaseCase
{

    /**
    * Check no exception is being built if no errors occured
    *
    */
    public function testNoError ()
    {
        $response = $this->getCliResponse('no_errors');

        $exception = $response->getException();

        $this->assertEquals(null, $exception);
    }//function testNoError()


    /**
    * Check no exception is being built if notice occured
    *
    */
    public function testNoExceptionsOnNotice ()
    {
        $response = $this->getCliResponse('notice');

        $exception = $response->getException();

        $this->assertEquals(null, $exception);
    }//function testNoExceptionsOnNotice()


    /**
    * Check no exception is being built if info message occured
    *
    */
    public function testNoExceptionsOnInfo ()
    {
        $response = $this->getCliResponse('info');

        $exception = $response->getException();

        $this->assertEquals(null, $exception);
    }//function testNoExceptionsOnInfo()


    /**
    * Check simple hdfs Exception is being built correctly if unknown error occured
    *
    */
    public function testUnknownError ()
    {
        $response = $this->getCliResponse('unknown_error');

        $exception = $response->getException();

        $success = ($exception ? get_class($exception) == 'Hdfs\Exception' : false);
        $this->assertTrue($success);
    }//function testUnknownError()


    /**
    * Check permission denided exception is being built correctly
    *
    */
    public function testPermissionDenied ()
    {
        $response = $this->getCliResponse('permission_denied');

        $exception = $response->getException();

        $success = ($exception instanceof \Hdfs\Exception\PermissionException);
        $this->assertTrue($success);
    }//function testPermissionDenied()


    /**
    * Check permission denided exception because of not being super user
    *
    */
    public function testPermissionDeniedNonSuperUser ()
    {
        $response = $this->getCliResponse('permission_not_root');

        $exception = $response->getException();

        $success = ($exception instanceof \Hdfs\Exception\PermissionException);
        $this->assertTrue($success);
    }//function testPermissionDeniedNonSuperUser()


    /**
    * Check NotFoundException is being built correctly
    *
    */
    public function testNotFound ()
    {
        $response = $this->getCliResponse('not_found');

        $exception = $response->getException();

        $success = ($exception instanceof \Hdfs\Exception\NotFoundException);
        $this->assertTrue($success);
    }//function testNotFound()


    /**
    * Check IllegalArgumentException is being built correctly
    *
    */
    public function testIllegalArgument ()
    {
        $response = $this->getCliResponse('illegal_argument');

        $exception = $response->getException();

        $success = ($exception instanceof \Hdfs\Exception\IllegalArgumentException);
        $this->assertTrue($success);
    }//function testIllegalArgument()


    /**
    * Check AlreadyExistsException is being built correctly
    *
    */
    public function testAlreadyExists ()
    {
        $response = $this->getCliResponse('already_exists');

        $exception = $response->getException();

        $success = ($exception instanceof \Hdfs\Exception\AlreadyExistsException);
        $this->assertTrue($success);
    }//function testAlreadyExists()


    /**
    * Check NotEmptyException is being built correctly
    *
    */
    public function testNotEmpty ()
    {
        $response = $this->getCliResponse('not_empty');

        $exception = $response->getException();

        $success = ($exception instanceof \Hdfs\Exception\NotEmptyException);
        $this->assertTrue($success);
    }//function testNotEmpty()


}//class CliResponseTest