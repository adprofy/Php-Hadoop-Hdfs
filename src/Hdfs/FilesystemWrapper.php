<?php
namespace Hdfs;



/**
* Wrap standart php filesystem functions used by `\hdfs` classes.
* Straithforward function invocation without any pre/post-processing.
*/
class FilesystemWrapper
{

    /**
    * Constructor
    *
    */
    public function __construct ()
    {

    }//function __construct()


    /**
    * file_exists()
    *
    * @param string $path
    *
    * @return bool
    */
    public function isExists ($path)
    {
        return file_exists($path);
    }//function isExists()


    /**
    * is_readable()
    *
    * @param string $path
    *
    * @return bool
    */
    public function isReadable ($path)
    {
        return is_readable($path);
    }//function isReadable()


    /**
    * is_writable()
    *
    * @param string $path
    *
    * @return bool
    */
    public function isWritable ($path)
    {
        return is_writable($path);
    }//function isWritable()


    /**
    * file_put_contents
    *
    * @param string $path
    * @param string $content
    */
    public function saveFile ($path, $content)
    {
        file_put_contents($path, $content);
    }//function saveFile()


    /**
    * is_dir()
    *
    * @param string $path
    *
    * @return bool
    */
    public function isDir ($path)
    {
        return is_dir($path);
    }//function isDir()


    /**
    * grol('*')
    *
    * @return array
    */
    public function readDir ($path)
    {
        $path  = preg_replace('#/$#', '', $path);
        $files = glob("$path/*");

        return $files;
    }//function readDir()


    /**
    * mkdir()
    *
    * @param string $path
    */
    public function makeDir ($path)
    {
        mkdir($path);
    }//function makeDir()


    /**
    * unlink()
    *
    * @param string $path
    */
    public function removeFile ($path)
    {
        unlink($path);
    }//function removeFile()


    /**
    * fopen()
    *
    * @param string $file
    * @param string $mode
    *
    * @return resource
    */
    public function openFile ($file, $mode = 'r')
    {
        return fopen($file, $mode);
    }//function openFile()


    /**
    * fseek()
    *
    * @param resource $handle
    * @param int $offset
    *
    * @return int
    */
    public function seek ($handle, $offset)
    {
        return fseek($handle, $offset);
    }//function seek()


    /**
    * fread()
    * Read $lenght bytes from $handle.
    *
    * @param resource $handle
    * @param int $length
    *
    * @return string
    */
    public function read ($handle, $length)
    {
        return fread($handle, $length);
    }//function read()


    /**
    * fwrite()
    *
    * @param resource $handle
    * @param string $content
    *
    * @return string
    */
    public function write ($handle, $content)
    {
        return fwrite($handle, $content);
    }//function write()


    /**
    * fclose()
    *
    * @param resource $handle
    *
    * @return bool
    */
    public function closeFile ($handle)
    {
        return fclose($handle);
    }//function closeFile()


    /**
    * filesize()
    *
    * @param string $file
    *
    * @return itn
    */
    public function size ($file)
    {
        return filesize($file);
    }//function size()



}//class FilesystemWrapper