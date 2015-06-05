<?php

use \Hdfs\Web\Method;


/**
* Check WebHDFS based implementation invoking physical data.
*
* :INFO:
* See defined constants
*
*/
class WebIntegrationTest extends PHPUnit_Framework_TestCase
{
    /** directories&files to run tests against */
    const REMOTE_DIR    = '/tmp/hdfs_cli_testDir';
    const REMOTE_SUBDIR = '/tmp/hdfs_cli_testDir/sub';
    const REMOTE_FILE   = '/tmp/hdfs_cli_testDir/file.txt';
    const LOCAL_DIR     = '/tmp/hdfs_cli_local_testDir';
    const LOCAL_SUBDIR  = '/tmp/hdfs_cli_local_testDir/sub';
    const LOCAL_FILE    = '/tmp/hdfs_cli_local_testDir/file.txt';

    /**
    * Construct & configure instance of \Hdfs\Web
    *
    * @return \Hdfs\Web
    */
    protected function getWebHdfs ()
    {
        $hdfs = new \Hdfs\Web($this->getWrapper());

        return $hdfs;
    }//function getWebHdfs()


    /**
    * Get curl wrapper
    *
    * @return \Hdfs\Web\WebHdfsWrapper
    */
    protected function getWrapper ()
    {
        return new \Hdfs\Web\WebHdfsWrapper('gate2.ovh.adprofy.com', 56000, 'test');
    }//function getWrapper()



    /**
    * Remove files&directories created for testing
    *
    */
    protected function clearEnvironment ()
    {
        clearstatcache();

        $this->getWrapper()->exec(Method::DELETE, self::REMOTE_DIR, 'DELETE', array('recursive' => 'true'));

        if (file_exists(self::LOCAL_SUBDIR))
        {
            rmdir(self::LOCAL_SUBDIR);
        }
        if (file_exists(self::LOCAL_FILE))
        {
            unlink(self::LOCAL_FILE);
        }
        if (file_exists(self::LOCAL_DIR))
        {
            rmdir(self::LOCAL_DIR);
        }

        clearstatcache();
    }//function clearEnvironment()


    /**
    * Remove created files/directories
    *
    */
    public function setUp ()
    {
        parent::setUp();

        $this->clearEnvironment();
    }//function setUp()


    /**
    * Remove created files/directories
    *
    */
    public function tearDown ()
    {
        parent::tearDown();

        $this->clearEnvironment();
    }//function tearDown()


    /**
    * Build local filesystem structure for tests
    *
    * Pass self::LOCAL_* constants as arguments
    */
    protected function createLocal ()
    {
        foreach (func_get_args() as $arg)
        {
            switch ($arg)
            {
                case self::LOCAL_DIR:
                case self::LOCAL_SUBDIR:
                        mkdir($arg, 0777, true);
                    break;
                case self::LOCAL_FILE:
                        file_put_contents(self::LOCAL_FILE, 'example content');
                    break;
            }
        }
    }//function createLocal()


    /**
    * Build hdfs filesystem structure for tests
    *
    * Pass self::REMOTE_* constants as arguments
    */
    protected function createRemote ($dir = true, $file = true, $subDir = true)
    {
        clearstatcache();

        foreach (func_get_args() as $arg)
        {
            switch ($arg)
            {
                case self::REMOTE_DIR:
                case self::REMOTE_SUBDIR:
                        $this->getWrapper()->exec(Method::PUT, $arg, 'MKDIRS');
                    break;
                case self::REMOTE_FILE:
                        $fname = '/tmp/TEMP_HDFS_FILE';
                        if (!file_exists($fname))
                        {
                            file_put_contents($fname, 'example hdfs');
                        }

                        $web = $this->getWrapper();
                        $uploadUrl = $web->getDatanodeUrl(Method::PUT, self::REMOTE_FILE, 'CREATE', array('overwrite' => 'true'));
                        $web->put($uploadUrl, $fname);
                    break;
            }
        }

        clearstatcache();
    }//function createRemote()


    /**
    * Check file uploading & downloading
    *
    */
    public function testPutGetFile ()
    {
        $hdfs = $this->getWebHdfs();
        $this->createLocal(self::LOCAL_DIR, self::LOCAL_FILE);
        $this->createRemote(self::REMOTE_DIR);

        $content = file_get_contents(self::LOCAL_FILE);
        $hdfs->putFile(self::LOCAL_FILE, self::REMOTE_FILE);
        unlink(self::LOCAL_FILE);
        clearstatcache();

        $this->assertFalse(file_exists(self::LOCAL_FILE));

        $hdfs->getFile(self::REMOTE_FILE, self::LOCAL_FILE);
        $downloaded = file_get_contents(self::LOCAL_FILE);

        $this->assertEquals($content, $downloaded);
    }//function testPutGetFile()


    /**
    * Check `makeDir()` `changeMode()` and `stat()`
    *
    */
    public function testMakeDirChangeModeStat ()
    {
        $hdfs = $this->getWebHdfs();

        $hdfs->makeDir(self::REMOTE_DIR);
        $hdfs->changeMode(self::REMOTE_DIR, 0777);
        $entry = $hdfs->stat(self::REMOTE_DIR);

        $this->assertEquals(basename(self::REMOTE_DIR), $entry->getName());
        $this->assertEquals(hdfs\EntryType::DIRECTORY, $entry->getType());
        $this->assertEquals(octdec('777'), $entry->getMode());
        $this->assertEquals(0, $entry->getSize());
        $this->assertTrue(time() - $entry->getMtime() < 20);
        $this->assertEquals('test', $entry->getOwner());
        $this->assertEquals('hdfs', $entry->getGroup());
    }//function testMakeDirChangeModeStat()


    /**
    * Check `removeFile()` when no errors occure
    *
    */
    public function testRemoveFile ()
    {
        $hdfs = $this->getWebHdfs();
        $this->createRemote(self::REMOTE_DIR, self::REMOTE_FILE);

        $hdfs->removeFile(self::REMOTE_FILE);

        $result = $this->getWrapper()->exec(Method::GET, self::REMOTE_FILE, 'GETFILESTATUS');
        $this->assertTrue($result->getException() instanceof \Hdfs\Exception\NotFoundException);
    }//function testRemoveFile()


    /**
    * Check `removeDir()` when no errors occure
    *
    */
    public function testRemoveDir ()
    {
        $hdfs = $this->getWebHdfs();
        $this->createRemote(self::REMOTE_DIR);

        $hdfs->removeDir(self::REMOTE_DIR);

        $result = $this->getWrapper()->exec(Method::GET, self::REMOTE_DIR, 'GETFILESTATUS');
        $this->assertTrue($result->getException() instanceof \Hdfs\Exception\NotFoundException);
    }//function testRemoveDir()

}//class WebIntegrationTest