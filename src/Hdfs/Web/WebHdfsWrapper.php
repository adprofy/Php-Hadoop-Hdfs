<?php
namespace Hdfs\Web;



/**
* Class to execute WebHDFS commands
*
*/
class WebHdfsWrapper
{
    /** @var string WebHDFS host */
    protected $host;
    /** @var int WebHDFS port */
    protected $port;
    /** @var string Username for WebHDFS connection */
    protected $user;
    /** @var \Closure Callback which accepts one argument: \Hdfs\Web\Response */
    protected $debugCallback = null;


    /**
    * Constructor
    *
    * @param string $host
    * @param int $port
    * @param null|string $user If `null` given, current php process owner will be used
    */
    public function __construct ($host = 'localhost', $port = 50070, $user = null)
    {
        $this->configure($host, $port, $user);
    }//function __construct()


    /**
    * Set connection parameters
    *
    * @param string $host
    * @param int $port
    * @param null|string $user If `null` given, current php process owner will be used
    */
    public function configure ($host, $port, $user = null)
    {
        $this->host = $host;
        $this->port = $port;
        $this->user = ($user ? : get_current_user());
    }//function configure()


    /**
    * Get configuration options
    *
    * @return array Associative array with keys host, port, user
    */
    public function getConfig ()
    {
        return array(
            'host' => $this->host,
            'port' => $this->port,
            'user' => $this->user
        );
    }//function getConfig()


    /**
    * Set callback wich will be invoked on each WebHDFS request.
    * Should accept one arguments:
    *   - \Hdfs\Web\Response - Result of WebHDFS command execution.
    *
    * @param Closure $callback
    */
    public function setDebugCallback (\Closure $callback)
    {
        $this->debugCallback = $callback;
    }//function setDebugCallback()


    /**
    * Before uploading a file we need to get datanode location
    *
    * @param string $httpMethod One of  \Hdfs\Web\Method constants
    * @param string $operation
    * @param array $arguments
    *
    * @throws \Hdfs\Exception If failed to obtain datanode url
    *
    * @return string
    */
    public function getDatanodeUrl ($httpMethod, $hdfsPath, $operation, array $arguments = array())
    {
        $url = $this->buildUrl($hdfsPath, $operation, $arguments);
        $options = array(
            CURLOPT_HEADER => true
        );
        switch ($httpMethod)
        {
            case Method::PUT:
                    $response = $this->put($url, null, $options);
                break;
            case Method::POST:
                    $response = $this->post($url, $options);
                break;
            //other methods don't need datanode urls
            default:
                return $url;
        }

        $datanodeUrl = $response->getRedirectUrl();
        if (!$datanodeUrl)
        {
            $msg = "No datanode url found in response for datanode url request:\n$response->body";
            throw new \Hdfs\exceptions\Exception($msg, true);
        }

        return $datanodeUrl;
    }//function getDatanodeUrl()


    /**
    * Execute WebHdfs operation
    *
    * @param string $httpMethod One of `\Hdfs\Web\Method` constants
    * @param string $hdfsPath
    * @param string $operation
    * @param array $arguments
    * @param null|string $localFile
    *
    * @return \Hdfs\Web\Response
    */
    public function exec ($httpMethod, $hdfsPath, $operation, array $arguments = array(), $localFile = null)
    {
        $url = $this->buildUrl($hdfsPath, $operation, $arguments);

        $response = null;
        switch ($httpMethod)
        {
            case Method::GET:
                    $response = $this->get($url);
                break;
            case Method::POST:
                    $result = $this->post($url);
                break;
            case Method::PUT:
                    $response = $this->put($url);
                break;
            case Method::DELETE:
                    $response = $this->delete($url);
                break;
        }

        return $response;
    }//function exec()


    /**
    * Perform PUT request
    *
    * @param string $url
    * @param null|string|resource $localFile If is string, file should exist otherwise result is unspecified
    * @param array $options Additional CURL options
    * @param int $length If $localFile is resource (opened file handle) pass amount of bytes to write using this argument.
    *
    * @return \Hdfs\Web\Response
    */
    public function put ($url, $localFile = null, array $options = array(), $length = 0)
    {
        $options[CURLOPT_URL]        = $url;
        $options[CURLOPT_PUT]        = true;
        $options[CURLOPT_HTTPHEADER] = array('Content-type: application/octet-stream');
        if ($localFile)
        {
            if (is_resource($localFile))
            {
                $options[CURLOPT_INFILE]     = $localFile;
                $options[CURLOPT_INFILESIZE] = $length;
            }
            else
            {
                $options[CURLOPT_INFILE]     = fopen($localFile, 'r');
                $options[CURLOPT_INFILESIZE] = filesize($localFile);
            }
        }
        else
        {
            $options[CURLOPT_INFILESIZE] = 0;
        }

        return $this->curl($options);
    }//function put()


    /**
    * Perform GET request
    *
    * @param string $url
    * @param array $options Additional CURL options
    *
    * @return \Hdfs\Web\Response
    */
    public function get ($url, array $options = array())
    {
        $options[CURLOPT_URL] = $url;
        $options[CURLOPT_FOLLOWLOCATION] = true;

        return $this->curl($options);
    }//function get()


    /**
    * Perform DELETE request
    *
    */
    public function delete ($url, array $options = array())
    {
        $options[CURLOPT_URL] = $url;
        $options[CURLOPT_CUSTOMREQUEST] = Method::DELETE;

        return $this->curl($options);
    }//function delete()


    /**
    * Generate request URL
    *
    * @param string $hdfsPath
    * @param string $operation
    * @param array $arguments
    */
    protected function buildUrl ($hdfsPath, $operation, array $arguments)
    {
        if ($hdfsPath[0] == '/')
        {
            $hdfsPath = substr($hdfsPath, 1);
        }

        $arguments['op']        = $operation;
        $arguments['user.name'] = $this->user;
        $queryString = http_build_query(array_filter($arguments));

        $url = "http://{$this->host}:{$this->port}/webhdfs/v1/$hdfsPath?$queryString";

        return $url;
    }//function buildUrl()


    /**
    * Execute CURL request
    *
    * @param array $options Curl configuration options
    *
    * @return \Hdfs\Web\Response
    */
    protected function curl (array $options)
    {
        $options[CURLOPT_RETURNTRANSFER] = true;

        $ch = curl_init();
        curl_setopt_array($ch, $options);

        $response = new Response();
        $response->body = curl_exec($ch);
        $response->info = (object)curl_getinfo($ch);

        curl_close($ch);

        if (!is_null($this->debugCallback))
        {
            $fn = $this->debugCallback;
            $fn(clone $response);
        }

        return $response;
    }//function curl()



}//class WebHdfsWrapper