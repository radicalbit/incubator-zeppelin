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

  private static  Logger LOGGER = LoggerFactory.getLogger(FlinkInterpreter.class);

  private ByteArrayOutputStream out;
  private LocalFlinkMiniCluster localFlinkCluster;
  private Configuration flinkConfiguration;
  private FlinkILoop flinkIloop;
  private Map<String, Object> binder;
  private IMain imain;

  public FlinkInterpreter(Properties property) {
    super(property);
  }

  static {
    Interpreter.register(
        "flink",
        "flink",
        FlinkInterpreter.class.getName(),
        new InterpreterPropertyBuilder()
          .add(FlinkInterpreterConsts.HOST,
               FlinkInterpreterConsts.LOCAL,
               "host name of running JobManager. 'local' runs flink in local mode")
          .add(FlinkInterpreterConsts.PORT,
               FlinkInterpreterConsts.DEFAULT_PORT,
               "port of running JobManager")
          .build()
    );
  }

  private static String sanitize(String property) {
    return Strings.nullToEmpty(property).trim();
  }

  /**
   * Load properties from a file
   * @param propertiesLocation file's path to load
   * @return properties from file
   */
  private static Properties fromFileToProperties(String propertiesLocation) {

    final File propertiesFile = new File(propertiesLocation);
    final Properties properties = new Properties();

    if (propertiesFile.exists()) {
      FileInputStream inputStream = null;
      try {
        inputStream = new FileInputStream(propertiesFile);
        properties.load(inputStream);
      } catch (IOException e) {
        LOGGER.error(e.getMessage(), e);
      } finally {
        if (inputStream != null) {
          try {
            inputStream.close();
          } catch (IOException e) {
            LOGGER.error(e.getMessage(), e);
          }
        }
      }
    } else {
      throw new IllegalArgumentException("Flink Interpreter cannot fetch properties.");
    }

    return properties;
  }

  /**
   *
   * @return FLINK_USER environment variable or the current user
   */
  private static String getFlinkUser() {

    final String userFlinkEnv = System.getenv(FlinkInterpreterConsts.FLINK_USER);
    return (userFlinkEnv == null)
            ?  System.getProperty(FlinkInterpreterConsts.SYSTEM_USER)
            : userFlinkEnv;
  }

  private String getHost() {
    return sanitize(getProperty(FlinkInterpreterConsts.HOST));
  }


  /**
   *  Load path of the Flink's configuration from environment variable or Zeppelin property
   * @return path of the Flink's configuration
   */
  private String getFlinkConfigurationPath() {

    final String flinkConfigurationEnv = System.getenv(FlinkInterpreterConsts.FLINK_CONF_DIR_ENV);
    return (flinkConfigurationEnv == null)
            ? sanitize(getProperty(FlinkInterpreterConsts.FLINK_CONF_DIR))
            : flinkConfigurationEnv;
  }

  /**
   * If so requested, it starts the local environment.
   * Retrieve host and port of JobManager according to set host into interpreter's properties
   * @return InetSocketAddress containing host and port of JobManager
   */
  private InetSocketAddress configureEnvironment() {

    final InetSocketAddress inetSocketAddress;
    final String host = getHost();

    switch (host){
        case FlinkInterpreterConsts.LOCAL:
        case "":
          startFlinkMiniCluster();
          inetSocketAddress =
                  new InetSocketAddress("localhost", localFlinkCluster.getLeaderRPCPort());
          break;
        case FlinkInterpreterConsts.YARN:
          Properties properties = getPropertiesFromYarnProperties();
          String addressInStr = properties.getProperty(
                  FlinkInterpreterConsts.YARN_PROPERTIES_JOBMANAGER_KEY);
          inetSocketAddress = ClientUtils.parseHostPortAddress(addressInStr);
          break;
        default:
          final int port = Integer.parseInt(getProperty(FlinkInterpreterConsts.PORT));
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
    return tmpDir + File.separator + FlinkInterpreterConsts.YARN_PROPERTIES_FILE + getFlinkUser();
  }

  private Properties getPropertiesFromYarnProperties() {
    final String yarnPropertiesLocation = yarnPropertiesLocation();
    return fromFileToProperties(yarnPropertiesLocation);
  }

  @Override
  public void open() {
    out = new ByteArrayOutputStream();

    final String flinkConfigurationPath = getFlinkConfigurationPath();
    if (flinkConfigurationPath.length() > 0 ) {
      GlobalConfiguration.loadConfiguration(flinkConfigurationPath);
      flinkConfiguration = GlobalConfiguration.getConfiguration();
    } else {
      // if flinkConfigurationPath is empty, it loads configuration from Interpreter's properties
      Properties intpProperty = getProperty();
      flinkConfiguration = new Configuration();
      for (Object k : intpProperty.keySet()) {
        String key = (String) k;
        String val = toString(intpProperty.get(key));
        flinkConfiguration.setString(key, val);
      }
    }

    final InetSocketAddress jmAddress = configureEnvironment();

    LOGGER.info("Configuration flink: " + GlobalConfiguration.getConfiguration().toString() );
    LOGGER.info("Interpreter attempts to connect at JobManager (" + jmAddress.toString() + ")");

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
    final StringBuilder classpath = new StringBuilder();

    final List<File> paths = currentClassPath();
    for (File f : paths) {
      if (classpath.length() > 0) {
        classpath.append(File.pathSeparator);
      }
      classpath.append(f.getAbsolutePath());
    }

    final URL[] urls = getClassloaderUrls();
    if (urls != null) {
      for (URL u : urls) {
        if (classpath.length() > 0) {
          classpath.append(File.pathSeparator);
        }
        classpath.append(u.getFile());
      }
    }

    Settings settings = new Settings();
    // set classpath
    PathSetting pathSettings = settings.classpath();
    pathSettings.v_$eq(classpath.toString());
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
    if (host.equals(FlinkInterpreterConsts.LOCAL) || host.equals("")) {
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

    System.arraycopy(lines, 0, linesToRun, 0, lines.length);

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
        LOGGER.info("Interpreter exception", e);
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
