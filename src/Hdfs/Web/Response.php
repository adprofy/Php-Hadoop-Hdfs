<?php
namespace Hdfs\Web;



/**
* Response of WebHDFS server
*
*/
class Response implements \Hdfs\IResponse {

    /** @var string Response body */
    public $body = '';
    /** @var StdClass Response info returned by curl_getinfo() */
    public $info = null;
    /** @var \Hdfs\Exception Cached exception instance */
    protected $exception = null;
    /** @var object|null Decoded body */
    protected $json = null;


    /**
    * Get URL which was used to perform request
    *
    * @return string
    */
    public function getUrl ()
    {
        return $this->info->url;
    }//function getUrl()


    /**
    * Get HTTP status code
    *
    * @return int
    */
    public function getHttpCode ()
    {
        return $this->info->http_code;
    }//function getHttpCode()


    /**
    * If server responded with redirect, this will return redirect url
    *
    * @return null|string
    */
    public function getRedirectUrl ()
    {
        return (isset($this->info->redirect_url) ? $this->info->redirect_url : null);
    }//function getRedirectUrl()


    /**
    * Check response for errors and build exceptions depending on errors types
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

        //this is downloaded file
        if ($this->isFile() && $this->getHttpCode() == 200)
        {
            return null;
        }

        //unexpected server response
        if (!$this->isJson())
        {
            $this->exception = new \Hdfs\Exception("Unexpected server response:\n{$this->body}", true);
        }
        else
        {
            $result = $this->getJson();
            if ($result)
            {
                if (isset($result->boolean) && !$result->boolean)
                {
                    throw new \Hdfs\Exception("Action failed. Reason: unknown. Url: {$this->info->url}", false);
                }
                if (isset($result->RemoteException))
                {
                    $error = $result->RemoteException;
                    switch ($error->exception)
                    {
                        case 'FileAlreadyExistsException':
                                $this->exception = new \Hdfs\Exception\AlreadyExistsException($error->message, true);
                            break;
                        case 'AccessControlException':
                                $this->exception = new \Hdfs\Exception\PermissionException($error->message, true);
                            break;
                        case 'FileNotFoundException':
                                $this->exception = new \Hdfs\Exception\NotFoundException($error->message, true);
                            break;
                        case 'RemoteException':
                            if (stripos($error->message, 'is not empty'))
                            {
                                $this->exception = new \Hdfs\Exception\NotEmptyException($error->message, true);
                                break;
                            }
                        default:
                            $this->exception = new \Hdfs\Exception("Unknown exceptions: {$this->body}", true);
                    }
                }
            }
        }

        return $this->exception;
    }//function getException()


    /**
    * Check if server sent 'Content-type: application/json' header
    *
    * @return bool
    */
    public function isJson ()
    {
        if (!isset($this->info->content_type))
        {
            return false;
        }

        return ($this->info->content_type == 'application/json');
    }//function isJson()


    /**
    * Get decoded body.
    *
    * @return object|null
    */
    public function getJson ()
    {
        if (!$this->json)
        {
            $this->json = json_decode($this->body);
        }

        return $this->json;
    }//function getJson()


    /**
    * Check if this response contains downloaded file
    *
    * @return bool
    */
    public function isFile ()
    {
        return $this->info->content_type == 'application/octet-stream';
    }//function isFile()


    /**
    * Get content of downloaded file
    *
    * @return null|string
    */
    public function getFileContent ()
    {
        return ($this->isFile() ? $this->body : null);
    }//function getFileContent()


}//class Response