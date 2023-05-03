package com.github.piyushpatel2005.pluralsight.basics;

import com.sun.applet2.preloader.event.ConfigEvent;
import com.typesafe.config.*;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.*;
import org.apache.beam.sdk.transforms.ParDo;

import java.io.*;

public class PipelineAndRunnerProperties1 {
//    public static void main(String[] args) {
//        PipelineOptions options = PipelineOptionsFactory.create();
//
//        System.out.println("Runner: " + options.getRunner().getName());
//        System.out.println("Job Name : " + options.getJobName());
//        System.out.println("Options ID: " + options.getOptionsId());
//        System.out.println("Stable Unique Name: " + options.getStableUniqueNames());
//        System.out.println("Temp Location: " + options.getTempLocation());
//        System.out.println("User Agent: " + options.getUserAgent());
//
//    }

    public interface GamePipelineOptions extends PipelineOptions {
        @Description("Game name")
        @Default.String("hi")
        String getGame();

        void setGame(String value);

    }

    public static void main(String[] args) throws FileNotFoundException {
        GamePipelineOptions options = PipelineOptionsFactory.fromArgs(args)
                .withValidation()
                .as(GamePipelineOptions.class);

        System.out.println("Env: " + options.getGame());
//        Config config = loadConfig(options);
//        System.out.println("Game: " + config.getString("game"));
//        System.out.println("Topic : " + config.getString("topic"));

        String sysConf = "system.administrator = ${who-knows}";

        Config original = ConfigFactory.parseString(sysConf)
                .resolveWith(ConfigFactory.parseString("who-knows = don"));
        System.out.println(original.getString("system.administrator"));

        File file = new File("src/main/resources/data.conf");
        InputStream inputStream = new FileInputStream(file);
        InputStreamReader reader = new InputStreamReader(inputStream);
        String game = options.getGame();
        String env = "prod";
        String jobConf = String.format("game=%s,env=%s", game, env);
        Config jobConfObj = ConfigFactory.parseString(jobConf).resolve();
        String configText = ConfigFactory.parseReader(reader)
                .withFallback(jobConfObj)
//                .resolveWith(ConfigFactory.parseString(jobConf))
                .resolve()
                .root()
                .render(ConfigRenderOptions.concise().setJson(false));
        System.out.println(configText);
//        Config conf = ConfigFactory.parseString(configText)
//                .resolveWith(ConfigFactory.parseString("game = amazon"));
//        System.out.println(conf.getString("env"));
//        System.out.println(conf.getString("topic"));

    }

    public static Config loadConfig(GamePipelineOptions options) {
//        Config conf = ConfigFactory.load("data.conf").resolve();
//        return conf.withFallback(ConfigValueFactory.fromAnyRef(options.getGame()));
        Config envConf = ConfigFactory.load("data.conf");
        Config gameConf = ConfigFactory.parseString("game:" + options.getGame());
//        return ConfigFactory.load()
//                .withValue("game", ConfigValueFactory.fromAnyRef(options.getGame()))
//                .withValue("env", ConfigValueFactory.fromAnyRef("stg"))
//                .withFallback(envConf)
//                .resolveWith(ConfigFactory.parseString("game="+options.getGame()));
//                .withFallback(gameConf)
//                .resolve();
        String config = "topic = stg_${game}";
        return ConfigFactory.parseString(config)
                .resolveWith(ConfigFactory.parseString("env=stg"))
                .resolveWith(ConfigFactory.parseString("game=game"));



    }
}
