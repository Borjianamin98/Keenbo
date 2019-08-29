package in.nimbo.config;

import in.nimbo.common.exception.LoadConfigurationException;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;

public class SparkConfig {
    private static final String CONFIG_NAME = "spark.properties";
    private int port;
    private int minEdge;
    private int maxEdge;
    private int filterEdge;
    private double minNode;
    private double maxNode;
    private double filterNode;
    private int minPasswordLength;
    private String loginError;
    private String usernameDuplicateError;
    private String usernameInvalidError;
    private String emailInvalidError;
    private String passwordWeakError;
    private String passwordUnlikeError;
    private String nameError;

    public static SparkConfig load() {
        SparkConfig sparkConfig = new SparkConfig();
        try {
            PropertiesConfiguration config = new PropertiesConfiguration(CONFIG_NAME);
            sparkConfig.setPort(config.getInt("spark.port"));
            sparkConfig.setMinNode(config.getDouble("graph.sites.nodes.min"));
            sparkConfig.setMaxNode(config.getDouble("graph.sites.nodes.max"));
            sparkConfig.setFilterNode(config.getDouble("graph.sites.nodes.filter"));
            sparkConfig.setMinEdge(config.getInt("graph.sites.edges.min"));
            sparkConfig.setMaxEdge(config.getInt("graph.sites.edges.max"));
            sparkConfig.setFilterEdge(config.getInt("graph.sites.edges.filter"));
            sparkConfig.setMinPasswordLength(config.getInt("auth.password.min"));
            sparkConfig.setLoginError(config.getString("auth.error.login"));
            sparkConfig.setUsernameDuplicateError(config.getString("auth.error.register.username.duplicate"));
            sparkConfig.setUsernameInvalidError(config.getString("auth.error.register.username.invalid"));
            sparkConfig.setEmailInvalidError(config.getString("auth.error.register.email.invalid"));
            sparkConfig.setPasswordWeakError(config.getString("auth.error.register.password.weak"));
            sparkConfig.setPasswordUnlikeError(config.getString("auth.error.register.password.unlike"));
            sparkConfig.setNameError(config.getString("auth.error.register.name"));
            return sparkConfig;
        } catch (ConfigurationException e) {
            throw new LoadConfigurationException(CONFIG_NAME, e);
        }
    }

    public String getNameError() {
        return nameError;
    }

    public void setNameError(String nameError) {
        this.nameError = nameError;
    }

    public String getPasswordUnlikeError() {
        return passwordUnlikeError;
    }

    public void setPasswordUnlikeError(String passwordUnlikeError) {
        this.passwordUnlikeError = passwordUnlikeError;
    }

    public String getPasswordWeakError() {
        return passwordWeakError;
    }

    public void setPasswordWeakError(String passwordWeakError) {
        this.passwordWeakError = passwordWeakError;
    }

    public String getEmailInvalidError() {
        return emailInvalidError;
    }

    public void setEmailInvalidError(String emailInvalidError) {
        this.emailInvalidError = emailInvalidError;
    }

    public String getUsernameInvalidError() {
        return usernameInvalidError;
    }

    public void setUsernameInvalidError(String usernameInvalidError) {
        this.usernameInvalidError = usernameInvalidError;
    }

    public String getUsernameDuplicateError() {
        return usernameDuplicateError;
    }

    public void setUsernameDuplicateError(String usernameDuplicateError) {
        this.usernameDuplicateError = usernameDuplicateError;
    }

    public String getLoginError() {
        return loginError;
    }

    public void setLoginError(String loginError) {
        this.loginError = loginError;
    }

    public int getMinPasswordLength() {
        return minPasswordLength;
    }

    public void setMinPasswordLength(int minPasswordLength) {
        this.minPasswordLength = minPasswordLength;
    }

    public int getMinEdge() {
        return minEdge;
    }

    public void setMinEdge(int minEdge) {
        this.minEdge = minEdge;
    }

    public int getMaxEdge() {
        return maxEdge;
    }

    public void setMaxEdge(int maxEdge) {
        this.maxEdge = maxEdge;
    }

    public int getFilterEdge() {
        return filterEdge;
    }

    public void setFilterEdge(int filterEdge) {
        this.filterEdge = filterEdge;
    }

    public double getMinNode() {
        return minNode;
    }

    public void setMinNode(double minNode) {
        this.minNode = minNode;
    }

    public double getMaxNode() {
        return maxNode;
    }

    public void setMaxNode(double maxNode) {
        this.maxNode = maxNode;
    }

    public double getFilterNode() {
        return filterNode;
    }

    public void setFilterNode(double filterNode) {
        this.filterNode = filterNode;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public int getPort() {
        return port;
    }
}
