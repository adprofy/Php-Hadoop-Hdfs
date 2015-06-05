<?php


/**
* Check `hadoop fs` cli based implementation invoking physical data.
*
* :INFO:
* See defined constants
*
*/
class CliIntegrationTest extends PHPUnit_Framework_TestCase
{
    /** directories&files to run tests against */
    const REMOTE_DIR    = '/tmp/hdfs_cli_testDir';
    const REMOTE_SUBDIR = '/tmp/hdfs_cli_testDir/sub';
    const REMOTE_FILE   = '/tmp/hdfs_cli_testDir/file.txt';
    const LOCAL_DIR     = '/tmp/hdfs_cli_local_testDir';
    const LOCAL_SUBDIR  = '/tmp/hdfs_cli_local_testDir/sub';
    const LOCAL_FILE    = '/tmp/hdfs_cli_local_testDir/file.txt';


    /**
    * Remove files&directories created for testing
    *
    */
    protected function clearEnvironment ()
    {
        clearstatcache();

        self::exec('-rm -r -f -skipTrash '. self::REMOTE_DIR);

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
    * Execute $cmd in via `hadoop fs` shell command.
    * Returns exit code.
    *
    * @param string $cmd
    * @param string $out - output written to STDOUT
    *
    * @return int
    */
    protected function exec ($cmd, &$out = null)
    {
        $exitCode = 0;
        $outArray = array();

        exec("hadoop fs $cmd", $outArray, $exitCode);

        $out = implode("\n", $outArray);

        return $exitCode;
    }//function exec()


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
    protected function createRemote ()
    {
        foreach (func_get_args() as $arg)
        {
            switch ($arg)
            {
                case self::REMOTE_DIR:
                case self::REMOTE_SUBDIR:
                        self::exec("-mkdir -p $arg");
                    break;
                case self::REMOTE_FILE:
                        self::exec("-touchz $arg");
                    break;
            }
        }
    }//function createRemote()


    /**
    * Check file uploading
    *
    */
    public function testPutFile ()
    {
        $this->createLocal(self::LOCAL_DIR, self::LOCAL_FILE);
        $this->createRemote(self::REMOTE_DIR);
        $hdfs = new \Hdfs\Cli();

        $hdfs->putFile(self::LOCAL_FILE, self::REMOTE_FILE);

        $success = !self::exec('-test -e '. self::REMOTE_FILE);
        $this->assertTrue($success);
    }//function testPutFile()


    /**
    * Check `getFile()` when no errors happen
    *
    */
    public function testGetFile ()
    {
        $hdfs = new \Hdfs\Cli();
        $this->createLocal(self::LOCAL_DIR);
        $this->createRemote(self::REMOTE_DIR, self::REMOTE_FILE);

        $hdfs->getFile(self::REMOTE_FILE, self::LOCAL_FILE);

        $success = file_exists(self::LOCAL_FILE);
        $this->assertTrue($success);
    }//function testGetFile()


    /**
    * Check `removeFile()` when no errors occure
    *
    */
    public function testRemoveFile ()
    {
        $hdfs = new \Hdfs\Cli();
        $this->createRemote(self::REMOTE_DIR, self::REMOTE_FILE);

        $hdfs->removeFile(self::REMOTE_FILE);

        $removed = self::exec('-test -e '. self::REMOTE_FILE);
        $this->assertEquals(1, $removed);
    }//function testRemoveFile()


    /**
    * Check `setFileReplication()` when no errors happen
    *
    */
    public function testSetFileReplication ()
    {
        $hdfs   = new hdfs\Cli();
        $factor = 10;
        $out    = '';
        $this->createRemote(self::REMOTE_DIR, self::REMOTE_FILE);

        $hdfs->setFileReplication(self::REMOTE_FILE, $factor);

        self::exec('-stat %r '. self::REMOTE_FILE, $out);
        $this->assertEquals($factor, $out);
    }//function testSetFileReplication()


    /**
    * Check `makeDir()` when directory is created successfully
    *
    */
    public function testMakeDir ()
    {
        $hdfs = new \Hdfs\Cli();

        $hdfs->makeDir(self::REMOTE_DIR);

        $success = !self::exec('-test -d '. self::REMOTE_DIR);
        $this->assertTrue($success);
    }//function testMakeDir()


    /**
    * Check `removeDir()` removes directory
    *
    */
    public function testRemoveDir ()
    {
        $hdfs = new \Hdfs\Cli();
        $this->createRemote(self::REMOTE_DIR);

        $hdfs->removeDir(self::REMOTE_DIR);

        $removed = self::exec('-test -e '. self::REMOTE_DIR);
        $this->assertEquals(1, $removed);
    }//function testRemoveDir()


    /**
    * Check `readDir()` when no errors occure
    *
    */
    public function testReadDir ()
    {
        $hdfs = new \Hdfs\Cli();
        $this->createRemote(self::REMOTE_DIR, self::REMOTE_FILE, self::REMOTE_SUBDIR);
        self::exec('-chmod 755 '. self::REMOTE_FILE);
        self::exec('-setrep 3 '. self::REMOTE_FILE);
        self::exec('-chmod 644 '. self::REMOTE_SUBDIR);

        $list = $hdfs->readDir(self::REMOTE_DIR);

        $amount = count($list);
        $this->assertEquals(2, $amount);

        foreach ($list as $entry)
        {
            switch ($entry->getName())
            {
                case basename(self::REMOTE_FILE):
                        $this->assertEquals(hdfs\EntryType::FILE, $entry->getType());
                        $this->assertEquals(octdec('755'), $entry->getMode());
                        $this->assertEquals(0, $entry->getSize());
                        $this->assertTrue(time() - $entry->getMtime() < 120); //cannot predict exact mtime due to slow start of `hadoop` utility
                        $this->assertTrue((bool)$entry->getOwner());
                        $this->assertEquals('hdfs', $entry->getGroup());
                        $this->assertEquals(3, $entry->getRfactor());
                    break;
                case basename(self::REMOTE_SUBDIR):
                        $this->assertEquals(hdfs\EntryType::DIRECTORY, $entry->getType());
                        $this->assertEquals(octdec('644'), $entry->getMode());
                        $this->assertEquals(0, $entry->getSize());
                        $this->assertTrue(time() - $entry->getMtime() < 120); //cannot predict exact mtime due to slow start of `hadoop` utility
                        $this->assertTrue((bool)$entry->getOwner());
                        $this->assertEquals('hdfs', $entry->getGroup());
                        $this->assertEquals(0, $entry->getRfactor());
                    break;
                default:
                    $this->assertTrue(false, "Unexpected entry in readDir() result: ". json_encode($entry->toArray()));
            }
        }
    }//function testReadDir()


    /**
    * Check `stat()` when no errors occure
    *
    */
    public function testStat ()
    {
        $hdfs = new \Hdfs\Cli();
        $this->createRemote(self::REMOTE_DIR, self::REMOTE_FILE);
        self::exec('-chmod 755 '. self::REMOTE_FILE);
        self::exec('-setrep 3 '. self::REMOTE_FILE);

        $entry = $hdfs->stat(self::REMOTE_FILE);

        $this->assertEquals(basename(self::REMOTE_FILE), $entry->getName());
        $this->assertEquals(hdfs\EntryType::FILE, $entry->getType());
        $this->assertEquals(octdec('755'), $entry->getMode());
        $this->assertEquals(0, $entry->getSize());
        $this->assertTrue(time() - $entry->getMtime() < 120); //cannot predict exact mtime due to slow start of `hadoop` utility
        $this->assertTrue((bool)$entry->getOwner());
        $this->assertEquals('hdfs', $entry->getGroup());
        $this->assertEquals(3, $entry->getRfactor());
    }//function testStat()


    /**
    * Check `changeMode()` when no errors occure
    *
    */
    public function testChangeMode ()
    {
        $hdfs = new \Hdfs\Cli();
        $this->createRemote(self::REMOTE_DIR);

        $hdfs->changeMode(self::REMOTE_DIR, 0777);
        $entry = $hdfs->stat(self::REMOTE_DIR);

        $this->assertEquals(octdec('777'), $entry->getMode());
    }//function testChangeMode()


}//class CliIntegrationTest