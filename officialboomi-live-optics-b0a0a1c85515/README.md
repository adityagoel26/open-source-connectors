For forking and pull requests. Ensure you are logged in with your Bitbucket account. Fork repository via the sitemap: https://confluence.atlassian.com/bitbucket/forking-a-repository-221449527.html
If fork is complete. Please submit a pull request. We'd love to review your code and consider it into the mainline
---

## Live Optics connector ##

The Live Optics connector enables you to connect to the Live Optics application and Get necessary data in JSON format using PAPIs and Project ID.

## Prerequisites ##
The following prerequisites are necessary to implement a connection to your account from Boomi Integration:

* Set the appropriate Java environment variables on your system, such as JAVA_HOME, CLASSPATH.
* Have your Live Optics API URL available to connect to your Live Optics account.
* To implement a connection to your Live Optics account, make sure an Atom is either deployed on your local machine or is hosted with Boomi Integration and have the following information:

        * Need a valid set of credentials with below three-string components used in Session Login 
		* Session Login ID: An ASCII string of characters that uniquely identifies the user
		* Session Login Secret: A base-64 encoded string that the user uses to authenticate themselves
		* Shared Secret: A second base-64 encoded string that the user uses to extract the session string from the Session Login
		> _**Note:**_ Both secrets should be treated like passwords. They should never reside as plaintext, but instead should be encrypted and locked down such that only the administrators have read access to the secrets and their encryption key. If the User does not have valid credentials, contact the management team.

* The Project ID - This is the ID of the project for which the project details are to be rendered.
* Configure Boomi Integration with valid user credentials and necessary permissions to perform the tasks necessary for the integration (such as being able to invoke RFCs and PAPIs, manipulate specific business objects, etc.).

## Supported Operations ##
The Live Optics operation defines how to interact with your Live Optics account and represents a specific action (GET) to perform against a specific record.

### Operation Details ###

### GET ###
GET Operation Used to set up a connector operation associated with PAPI. It will fetch necessary data in JSON format using PAPIs and Project ID.

## UserGuide ##
https://help.boomi.com/bundle/connectors/page/int-Live_Optics_connector.html

## Additional resources ##
https://support.liveoptics.com/hc/en-us/articles/229590067-Live-Optics-Quick-Start-Guide
https://support.liveoptics.com/hc/en-us/articles/236171228-Live-Optics-End-User-Information-Guide