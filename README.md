# bejmx
This Java utility supports the monitoring of [TIBCO BusinessEvents](https://docs.tibco.com/products/tibco-businessevents-5-2-0) (BE) applications.  It implemented a JMX client to fetch operational stats from BE inference engines, and writes the stats to a file in a pre-configured interval.  Note that this utility monitors BE inference engines.  To monitor BE cache engines that use [TIBCO ActiveSpaces](https://docs.tibco.com/products/tibco-activespaces-enterprise-edition-2-1-5), you can use the [asmonitor](https://github.com/yxuco/asmonitor).

## Dependencies
This utility uses standard JMX API, and thus does not depend on any proprietary packages.
 
#### Maven

This is a Maven project, and so if Maven has not been installed on your system, you'll need to install Maven and Git as described in the [beunit](https://github.com/yxuco/beunit) project.
    
#### Clone this project from GitHub

In the root folder of your workspace, clone the project using command

    git clone https://github.com/yxuco/bejmx.git

It should download the source code to the folder `bejmx` in your workspace. 

## Build the utility

In your workspace,

    cd bejmx
    mvn clean package

The Maven build should be successful.  This step builds `bejmx-2.1.jar` in the folder `$WORKSPACE/bejmx/target/`.  In the same folder, you can find a sample configuration and a script illustrating how to use this utility:

    bejmx.sh - it shows how to start the utility (you can edit it to match your working environment.)
    config.properties - illustrates properties that can be edited to monitor multiple BE inference engines.
    
The configurable properties are explained in the sample file.  Multi-value properties can be specified using a unique suffix.  For example, the following 3 properties tell the utility to generate 3 different reports (each report is written in a separate file.)

    report.1 BEEntityCache
    report.2 BEAgentEntity
    report.3 RTCTxnManagerReport
    
Similarly, you may specify multiple BE inference engines with unique JMX port and engine name, and so all of them are monitored. 

## Development using Eclipse
 
You may also edit and build the utility using Eclipse.

 - Launch Eclipse.
 - Pulldown **File** menu and select **Import...**
 - In the **Import** dialog, select **Existing Maven Projects**, then click **Next >** button.
 - In the **Import Maven Projects** dialog, browse for **Root Directory**, select and open the `bejmx` folder under your workspace.
 - Confirm that `your-workspace/bejmx` is populated as the **Root Directory**, then click the **Finish** button.
 - In **Package Explorer**, highlight the root folder of the imported project `bejmx`, and pulldown **Window** menu to open the Java Perspective.

## The author

Yueming is a Sr. Architect working at [TIBCO](http://www.tibco.com/) Architecture Service Group.