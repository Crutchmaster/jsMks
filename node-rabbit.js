var amqp = require("amqplib/callback_api");
var request = require("request");
var repl = require("repl");
var channel = {}, conn, res;
var app = {};

var args = {
    rmqport : 5672,
    rmqwebport : 15672
};

for (var i in process.argv) {
    var arg = process.argv[i];
    var kv;
    if (arg.search("--") == 0) {
        arg = arg.substr(2);
        kv = arg.split("=");
        args[kv[0]] = kv[1] ? kv[1] : true;
    }
}

if (!args.rmqhost || !args.rmquser || !args.rmqpassword) {
    args.help = true;
}


if (args.help) {
    console.log("NodeJS mircoservice crutchbag (aka swedish knife)\n"
               +"Arguments:\n"  
               +"--rmqhost=<host> rabbitmq host\n"
               +"--rmquser=<username> rabbitmq username\n"
               +"--rmqpassword=<pwd> rabbitmq password\n"
               +"Optional\n"
               +"--rmqport=<port> rabbitmq port (default:5672)\n"
               +"--rmqwebport=<web port> web api port (default:15672)\n"
               +"Cheers.\n"
               );
    process.exit();
}

var replServer = repl.start({ prompt: "> ",
                              useGlobal : true});
var ctx = replServer.context;
var q = {};

var ctxAppName = {
    template : "tpl",
}


function log(s) {
    console.log(s);
    ctx.retStr = s;
    ctx.ret = {}
    try {
        ctx.ret = JSON.parse(s);
    } catch (err) {}
}

function collector(s) {
    try {
        var r = JSON.parse(s)
        ctx.retArr.push(r);
        console.log(ctx.retArr.length);
    } catch (err) {}
}

function Queue(n, ch) {
    this.name = n;
    this.channel = ch;
    this.send = function(msg) {
        this.channel.sendToQueue(this.name, new Buffer(msg));
    }
    this.get = function(callback) {
        var ch = this.channe;
        this.channel.get(this.name, false, function(err, msg) {
            if (msg) callback(msg.content.toString());
            ch.ask(msg);
        });
    }
    this.consume = function(callback) {
        this.stopConsume();
        var ch = this.channel;
        this.channel.consume(this.name, function(msg) {
                callback(msg.content.toString());
                ch.ack(msg);
            }, 
            {consumerTag : this.name, exclusive: true});
    }
    this.stopConsume = function() {
        this.channel.cancel(this.name);
    }
}

function onConnect(error, c) {
    if (error != null) {
        log(error);
        return;
    }
    conn = c;
    conn.createChannel(function(err, ch) {
        if (err != null) {log(err); return;}
        channel = ch;
        ctx.ch = ch;
        getQueues(ch);
        log("AMQP connected");
    });
}

function getQueues(ch) {
        var options = {
                url : "http://"+args.rmqhost+":"+args.rmqwebport+"/api/queues",
                auth : {
                    user : args.rmquser,
                    password : args.rmqpassword
                },
                json : true,
                headers : {
                      "Content-Type" : "application/json"
                },
            }
        request.get(options,
            function(e,r,b) {
                loadQueues(b, ch);
                log("Queues loaded");
                autoLoadApps();
            });
        }

function loadQueues(data, ch) {
    for (var i = 0; i < data.length; i++) {
        var r = data[i];
        q[r.name] = new Queue(r.name, ch);
    }
}

function autoLoadApps() {
    for (var k in q) {
        if (k.search("_control")) {
            var appName = k.replace("_control","");
            if (q[appName+"_out"] && q[appName+"_log"]) {
                loadApp(appName);
            }
        }
    }
}

function toStr(s) {return JSON.stringify(s);}

function App(name, qin, qout, qerr) {
    this.name = name;
    this.commands = [];
    this.qin = qin;
    this.qout = qout;
    this.qerr = qerr;
    this.getmsg = {};
    this.msgwait = 0;
    this.cmd = {};
    this.reload = function() {
        if (!this.qin) {
            log("Not found input queue for "+this.name);
            return false;
        }
        if (!this.qout) {
            log("Not found output queue for "+this.name);
            return false;
        }
        log("Reload "+this.name);
        this.msgwait = 2;
        this.getmsg.appName = function(msg, app) {app.name = msg.appName; app.msgwait--; app.postLoad();}
        this.getmsg.commands = function(msg, app) {app.commands = msg.commands; app.msgwait--; app.postLoad();}
        var listenerFunc = function(app) {
                    return function(msg) {app.loader(msg);} 
                    }(this);
        this.setListener(listenerFunc);
        this.qin.send(toStr({getCommands:[]}));
        this.qin.send(toStr({getName:[]}));
    }
    this.loader = function(msg) {
        var msgobj = {};

        try {msgobj = JSON.parse(msg);} 
        catch (err) {log("Parse error for app "+this.name+" msg:"+msg);}
        for (var key in msgobj) {
            if (this.getmsg[key]) {
                this.getmsg[key](msgobj, this);
                this.getmsg[key] = false;
            }
        }
       
    }
    this.postLoad = function() {
        if (this.msgwait == 0) {
            this.setListener(log);
            for (var i = 0; i < this.commands.length; i++) {
                var cmd = this.commands[i];
                var cmdFunction = function(cmdname, qin, qout) {
                    return function() {
                        var sendobj = {};
                        var args = [];
                        for (var key in arguments) args.push(arguments[key]);
                        sendobj[cmdname] = args;
                        qin.send(JSON.stringify(sendobj));
                        }
                    }(cmd, this.qin, this.qout)

                if (!this[cmd]) this[cmd] = cmdFunction;
                if (!this.cmd[cmd]) this.cmd[cmd] = cmdFunction;
                }
             log(this.name+" loaded.");
             checkAppContext(this);
        }
    }
    this.setListener = function(fun) {
        this.qout.consume(fun);
    }
    this.showErrors = function() {
        this.qerr.consume(log);
    }
    this.hideErrors = function() {
        this.qerr.stopConsume();
    }
}

function loadApp(name) {
    var qinName = name+"_control";
    var qoutName = name+"_out";
    var qlogName = name+"_log";
    var checks = [qinName, qoutName, qlogName];
    for (var i in checks) {
        var chk = checks[i];
        if (!q[chk]) {
            log("Error: quene "+chk+" not found.");
            return;
        }
    }
    app[name] = new App(name, q[qinName], q[qoutName], q[qlogName]);
    app[name].reload();
}

function checkAppContext(app) {
    if (ctxAppName[app.name]) {
        ctx[ctxAppName[app.name]] = app;
        log("App "+app.name+" linked to "+ctxAppName[app.name]);
    }
}


var connectstr = "amqp://"+args.rmquser+":"+args.rmqpassword+"@"+args.rmqhost+":"+args.rmqport;
amqp.connect(connectstr, onConnect);
ctx.q = q;
ctx.loadApp = loadApp;
ctx.log = log;
ctx.collector = collector;
ctx.toStr = toStr;
ctx.app = app;
ctx.reloadApps = function() {autoLoadApps()};
ctx.getQueues = function() {getQueues(ctx.ch)}
ctx.retArr = [];
ctx.args = args;

ctx.clipArr = function() {
    child_process.exec("xsel -b", 
            { 
                maxBuffer : 1024*1024*100 //100 MB
            }, 
        function(err, s, e) {
            ctx.ret = s.toString().split("\n");
            ctx.retArr = [];
            var newob;
            for (var i = 0, c = ctx.ret.length; i < c; i++) {
                newob = false;
                try {newob = JSON.parse(ctx.ret[i]);} 
                catch (err) {}
                if (newob) ctx.retArr.push(newob);
            }
    })
}
