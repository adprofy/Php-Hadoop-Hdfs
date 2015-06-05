<?php


/**
* Base class for tests
*
*/
class BaseCase extends PHPUnit_Framework_TestCase
{
    /** @var array Loaded fixtures for responses */
    static protected $responseData = array();

    /**
    * Get mock \Hdfs\Cli\Response of specified $type
    *
    * @param string $type
    *
    * @return \Hdfs\Cli\Response
    */
    public function getCliResponse ($type)
    {
        $response = new \Hdfs\Cli\Response();
        $this->populateResponse($response, $type);

        return $response;
    }//function getCliResponse()


    /**
    * Get mock \Hdfs\Web\Response of specified $type
    *
    * @param string $type
    *
    * @return \Hdfs\Web\Response
    */
    public function getWebResponse ($type)
    {
        $response = new \Hdfs\Web\Response();
        $this->populateResponse($response, $type);

        return $response;
    }//function getWebResponse()


    /**
    * Fill mock data for \Hdfs\Cli\Response or \Hdfs\Web\Response of specified $type
    *
    * @param string $type
    */
    protected function populateResponse (\Hdfs\IResponse $response, $type)
    {
        $reflector = new ReflectionClass(get_class($this));
        $testDir   = dirname($reflector->getFileName());
        $file    = "$testDir/_responses";

        if ($response instanceof \Hdfs\Cli\Response)
        {
            $file .= '/cli.json';
        }
        else
        {
            $file .= '/web.json';
        }

        if (!isset(self::$responseData[$file]))
        {
            self::$responseData[$file] = json_decode(file_get_contents($file));
            if (!self::$responseData[$file])
            {
                trigger_error("Unable to parse json file: $file", E_USER_ERROR);
            }
        }

        $data = self::$responseData[$file]->$type;
        foreach ($data as $key => $value)
        {
            $response->$key = $value;
        }
    }//function populateResponse()

}//class BaseCase