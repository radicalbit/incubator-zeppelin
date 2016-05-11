/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.zeppelin.flink;

import java.io.*;
import java.net.*;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.flink.api.scala.FlinkILoop;
import org.apache.flink.client.ClientUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.hadoop.shaded.com.google.common.base.Strings;
import org.apache.flink.runtime.minicluster.LocalFlinkMiniCluster;

import org.apache.zeppelin.interpreter.Interpreter;
import org.apache.zeppelin.interpreter.InterpreterContext;
import org.apache.zeppelin.interpreter.InterpreterPropertyBuilder;
import org.apache.zeppelin.interpreter.InterpreterResult;
import org.apache.zeppelin.interpreter.InterpreterResult.Code;
import org.apache.zeppelin.interpreter.InterpreterUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.Console;
import scala.Option;
import scala.None;
import scala.Some;
import scala.runtime.AbstractFunction0;
import scala.tools.nsc.Settings;
import scala.tools.nsc.interpreter.IMain;
import scala.tools.nsc.interpreter.NamedParamClass;
import scala.tools.nsc.interpreter.Results;
import scala.tools.nsc.settings.MutableSettings.BooleanSetting;
import scala.tools.nsc.settings.MutableSettings.PathSetting;

/**
 * Interpreter for Apache Flink (http://flink.apache.org)
 */
public class FlinkInterpreter extends Interpreter {

  private static  Logger logger = LoggerFactory.getLogger(FlinkInterpreter.class);

  private ByteArrayOutputStream out;
  private LocalFlinkMiniCluster localFlinkCluster;
  private Configuration flinkConfiguration;
  private FlinkILoop flinkIloop;
  private Map<String, Object> binder;
  private IMain imain;

  public FlinkInterpreter(Properties property) {
    super(property);
  }

  private static final String YARN_PROPERTIES_FILE = ".yarn-properties-";
  private static final String YARN_PROPERTIES_JOBMANAGER_KEY = "jobManager";
  private static final String FLINK_CONF_DIR_ENV = "FLINK_CONF_DIR";
  private static final String FLINK_USER_ENV = "FLINK_USER_ENV";
  private static final String HOST = "host";
  private static final String FLINK_CONF_DIR = "flink.conf.dir";
  private static final String FLINK_USER = "flink.user";
  static {
    Interpreter.register(
        "flink",
        "flink",
        FlinkInterpreter.class.getName(),
        new InterpreterPropertyBuilder()
          .add("host", "local", "host name of running JobManager. 'local' runs flink in local mode")
          .add("port", "6123", "port of running JobManager")
          .build()
    );
  }

  private static String sanitize(String property) {
    return Strings.nullToEmpty(property).trim();
  }

  private static Properties fromFileToProperties(String propertiesLocation) {

    final File propertiesFile = new File(propertiesLocation);
    final Properties properties = new Properties();

    if (propertiesFile.exists()) {
      FileInputStream inputStream = null;
      try {
        inputStream = new FileInputStream(propertiesFile);
        properties.load(inputStream);
      } catch (IOException e) {
        logger.error(e.getMessage(), e);
      } finally {
        if (inputStream != null) {
          try {
            inputStream.close();
          } catch (IOException e) {
            logger.error(e.getMessage(), e);
          }
        }
      }
    } else {
      throw new IllegalArgumentException("Flink Interpreter cannot fetch properties.");
    }

    return properties;
  }

  private String getUser() {

    final String userProp = sanitize(getProperty(FLINK_USER));
    final String user = (userProp.length() > 0)
            ? userProp
            : System.getenv(FLINK_USER_ENV);

    return (user == null) ? System.getenv("user.name") : user;
  }

  private String getHost() {
    return sanitize(getProperty(HOST));
  }

  private String getFlinkConfiguration() {

    final String flinkConfigurationProp = sanitize(getProperty(FLINK_CONF_DIR));
    return (flinkConfigurationProp.length() > 0)
            ? flinkConfigurationProp
            : System.getenv(FLINK_CONF_DIR_ENV);
  }

  private InetSocketAddress configureEnvironment() {

    final InetSocketAddress inetSocketAddress;
    final String host = getHost();

    switch (host){
        case "local":
        case "":
          startFlinkMiniCluster();
          inetSocketAddress =
                  new InetSocketAddress("localhost", localFlinkCluster.getLeaderRPCPort());
          break;
        case "yarn":
          Properties properties = getPropertiesFromYarnProperties();
          String addressInStr = properties.getProperty(YARN_PROPERTIES_JOBMANAGER_KEY);
          inetSocketAddress = ClientUtils.parseHostPortAddress(addressInStr);
          break;
        default:
          final int port = Integer.parseInt(getProperty("port"));
          inetSocketAddress = new InetSocketAddress(host, port);
          break;
    }

    return inetSocketAddress;
  }

  private String yarnPropertiesLocation() {
    final String defaultPropertiesFileLocation = System.getProperty("java.io.tmpdir");
    final String tmpDir = flinkConfiguration.getString(
            ConfigConstants.YARN_PROPERTIES_FILE_LOCATION,
            defaultPropertiesFileLocation);
    return tmpDir + File.separator + YARN_PROPERTIES_FILE + getUser();
  }

  private Properties getPropertiesFromYarnProperties() {
    final String yarnPropertiesLocation = yarnPropertiesLocation();
    return fromFileToProperties(yarnPropertiesLocation);
  }

  @Override
  public void open() {
    out = new ByteArrayOutputStream();

    final String flinkConfigurationPath = getFlinkConfiguration();
    if (flinkConfigurationPath.length() > 0 ){
      GlobalConfiguration.loadConfiguration(flinkConfigurationPath);
      flinkConfiguration = GlobalConfiguration.getConfiguration();
    } else {
      Properties intpProperty = getProperty();
      flinkConfiguration = new Configuration();
      for (Object k : intpProperty.keySet()) {
        String key = (String) k;
        String val = toString(intpProperty.get(key));
        flinkConfiguration.setString(key, val);
      }
    }

    final InetSocketAddress jmAddress = configureEnvironment();

    logger.info("Configuration flink: " + GlobalConfiguration.getConfiguration().toString() );
    logger.info("Interpreter attempts to connect at JobManager (" + jmAddress.toString() + ")");

    flinkIloop = new FlinkILoop(
            jmAddress.getHostString(),
            jmAddress.getPort(),
            flinkConfiguration,
            new Some<>(new String[]{}),
            (Option<BufferedReader>) null,
            new PrintWriter(out));

    flinkIloop.settings_$eq(createSettings());
    flinkIloop.createInterpreter();
    
    imain = flinkIloop.intp();

    org.apache.flink.api.scala.ExecutionEnvironment env = flinkIloop.scalaBenv();
    env.getConfig().disableSysoutLogging();

    // prepare bindings
    imain.interpret("@transient var _binder = new java.util.HashMap[String, Object]()");
    binder = (Map<String, Object>) getValue("_binder");    

    // import libraries
    imain.interpret("import scala.tools.nsc.io._");
    imain.interpret("import Properties.userHome");
    imain.interpret("import scala.compat.Platform.EOL");
    
    imain.interpret("import org.apache.flink.api.scala._");
    imain.interpret("import org.apache.flink.api.common.functions._");
    imain.bind(new NamedParamClass("env",
            "org.apache.flink.api.scala.ExecutionEnvironment", env));
    //imain.bindValue("env", env);
  }

  private Settings createSettings() {
    URL[] urls = getClassloaderUrls();
    Settings settings = new Settings();

    // set classpath
    PathSetting pathSettings = settings.classpath();
    String classpath = "";
    List<File> paths = currentClassPath();
    for (File f : paths) {
      if (classpath.length() > 0) {
        classpath += File.pathSeparator;
      }
      classpath += f.getAbsolutePath();
    }

    if (urls != null) {
      for (URL u : urls) {
        if (classpath.length() > 0) {
          classpath += File.pathSeparator;
        }
        classpath += u.getFile();
      }
    }

    pathSettings.v_$eq(classpath);
    settings.scala$tools$nsc$settings$ScalaSettings$_setter_$classpath_$eq(pathSettings);
    settings.explicitParentLoader_$eq(new Some<ClassLoader>(Thread.currentThread()
        .getContextClassLoader()));
    BooleanSetting b = (BooleanSetting) settings.usejavacp();
    b.v_$eq(true);
    settings.scala$tools$nsc$settings$StandardScalaSettings$_setter_$usejavacp_$eq(b);
    
    return settings;
  }
  

  private List<File> currentClassPath() {
    List<File> paths = classPath(Thread.currentThread().getContextClassLoader());
    String[] cps = System.getProperty("java.class.path").split(File.pathSeparator);
    if (cps != null) {
      for (String cp : cps) {
        paths.add(new File(cp));
      }
    }
    return paths;
  }

  private List<File> classPath(ClassLoader cl) {
    List<File> paths = new LinkedList<File>();
    if (cl == null) {
      return paths;
    }

    if (cl instanceof URLClassLoader) {
      URLClassLoader ucl = (URLClassLoader) cl;
      URL[] urls = ucl.getURLs();
      if (urls != null) {
        for (URL url : urls) {
          paths.add(new File(url.getFile()));
        }
      }
    }
    return paths;
  }

  public Object getValue(String name) {
    IMain imain = flinkIloop.intp();
    Object ret = imain.valueOfTerm(name);
    if (ret instanceof None) {
      return null;
    } else if (ret instanceof Some) {
      return ((Some) ret).get();
    } else {
      return ret;
    }
  }

  @Override
  public void close() {
    flinkIloop.closeInterpreter();

    final String host = getHost();
    if (host.equals("local") || host.equals("")) {
      stopFlinkMiniCluster();
    }
  }

  @Override
  public InterpreterResult interpret(String line, InterpreterContext context) {
    if (line == null || line.trim().length() == 0) {
      return new InterpreterResult(Code.SUCCESS);
    }

    InterpreterResult result = interpret(line.split("\n"), context);
    return result;
  }

  public InterpreterResult interpret(String[] lines, InterpreterContext context) {
    final IMain imain = flinkIloop.intp();
    
    String[] linesToRun = new String[lines.length + 1];
    for (int i = 0; i < lines.length; i++) {
      linesToRun[i] = lines[i];
    }
    linesToRun[lines.length] = "print(\"\")";

    System.setOut(new PrintStream(out));
    out.reset();
    Code r = null;

    String incomplete = "";
    for (int l = 0; l < linesToRun.length; l++) {
      final String s = linesToRun[l];
      // check if next line starts with "." (but not ".." or "./") it is treated as an invocation
      if (l + 1 < linesToRun.length) {
        String nextLine = linesToRun[l + 1].trim();
        if (nextLine.startsWith(".") && !nextLine.startsWith("..") && !nextLine.startsWith("./")) {
          incomplete += s + "\n";
          continue;
        }
      }

      final String currentCommand = incomplete;

      scala.tools.nsc.interpreter.Results.Result res = null;
      try {
        res = Console.withOut(
          System.out,
          new AbstractFunction0<Results.Result>() {
            @Override
            public Results.Result apply() {
              return imain.interpret(currentCommand + s);
            }
          });
      } catch (Exception e) {
        logger.info("Interpreter exception", e);
        return new InterpreterResult(Code.ERROR, InterpreterUtils.getMostRelevantMessage(e));
      }

      r = getResultCode(res);

      if (r == Code.ERROR) {
        return new InterpreterResult(r, out.toString());
      } else if (r == Code.INCOMPLETE) {
        incomplete += s + "\n";
      } else {
        incomplete = "";
      }
    }

    if (r == Code.INCOMPLETE) {
      return new InterpreterResult(r, "Incomplete expression");
    } else {
      return new InterpreterResult(r, out.toString());
    }
  }

  private Code getResultCode(scala.tools.nsc.interpreter.Results.Result r) {
    if (r instanceof scala.tools.nsc.interpreter.Results.Success$) {
      return Code.SUCCESS;
    } else if (r instanceof scala.tools.nsc.interpreter.Results.Incomplete$) {
      return Code.INCOMPLETE;
    } else {
      return Code.ERROR;
    }
  }



  @Override
  public void cancel(InterpreterContext context) {
  }

  @Override
  public FormType getFormType() {
    return FormType.NATIVE;
  }

  @Override
  public int getProgress(InterpreterContext context) {
    return 0;
  }

  @Override
  public List<String> completion(String buf, int cursor) {
    return new LinkedList<String>();
  }

  private void startFlinkMiniCluster() {
    localFlinkCluster = new LocalFlinkMiniCluster(flinkConfiguration, false);

    try {
      localFlinkCluster.start(true);
    } catch (Exception e){
      throw new RuntimeException("Could not start Flink mini cluster.", e);
    }
  }

  private void stopFlinkMiniCluster() {
    if (localFlinkCluster != null) {
      localFlinkCluster.stop();
      localFlinkCluster = null;
    }
  }

  static final String toString(Object o) {
    return (o instanceof String) ? (String) o : "";
  }
}
