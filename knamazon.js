var Amazon=require('knuty');
var AWS=require('aws-sdk');
var Fs=require('fs');

Amazon.extend({
  REC: [],
  open: function(cfg){
    if(this.CFG){this.info();}
    cfg=cfg||this.CFG.aws.cfg; AWS.config.loadFromPath(cfg);
  },
//ok
  listBuckets: function(op){
    var me=this; var s3=new AWS.S3(); var pm={}; op=op||{};
    var wid=me.ready(); var rc;
    s3.listBuckets(pm, function(err, data){
      if(err){rc=false; me.error=err;}else{rc=data;}
      me.post(wid);
    });
    me.wait(); return rc;
  },
//ok
  createBucket: function(bucket, op){
    var me=this; var s3=new AWS.S3(); var pm={}; op=op||{};
    pm.Bucket=bucket; pm.ACL='private';
    var wid=me.ready(); var rc;
    s3.createBucket(pm, function(err, data){
      if(err){rc=false; me.error=err;}else{rc=data;}
      me.post(wid);
    });
    me.wait(); return rc;
  },
//ok
  listObjects: function(bucket, op){
    var me=this; var s3=new AWS.S3(); var pm={}; op=op||{};
    pm=op; pm.Bucket=bucket;
    var wid=me.ready(); var rc;
    s3.listObjects(pm, function(err, data){
      if(err){rc=false; me.error=err;}else{rc=data;}
      me.post(wid);
    });
    me.wait(); return rc;
  },
//ok
  getObject: function(bucket, key, op){
    var me=this; var s3=new AWS.S3(); var pm={}; op=op||{};
    pm=op; pm.Bucket=bucket; pm.Key=key;
    var wid=me.ready(); var rc;
    s3.getObject(pm, function(err, data){
      if(err){rc=false; me.error=err;}else{rc=data.Body.toString();}
      me.post(wid);
    });
    me.post(); return rc;
  },
//ok
  upload: function(bucket, key, fpath, op){
    var me=this; var s3=new AWS.S3(); op=op||{}; var pm={};
    var stream=Fs.createReadStream(fpath);
    pm.Bucket=bucket; pm.Key=key;  pm.Body=stream;
    var wid=me.ready();
    s3.putObject(pm, function(err, data) {
      if (err){console.log(err)}
      if(err){rc=false; me.error=err;}else{rc=data;}
      me.post(wid);
    });
    me.wait(); return rc;
  },
//ok
  download: function(bucket, key, fpath, op){
    var me=this; var s3=new AWS.S3(); op=op||{}; var pm={};
    var stream=Fs.createWriteStream(fpath);
    pm.Bucket=bucket; pm.Key=key;
    var wid=me.ready();
    s3.getObject(pm, function(err, data){
      if (err){console.log(err)}
      if(err){rc=false; me.error=err;}else{stream.end(data.Body); rc=true;}
      me.post(wid);
    });
    me.wait(); return rc;
  },
//
  start: function(id, op){
    var me=this; var ec2=new AWS.EC2(); var pm={}; op=op||{};
    pm=op; pm.InstanceIds=[id];
    var wid=me.ready(); var rc;
    ec2.startInstances(pm, function(err, data){
      if(err){rc=false; me.error=err;}else{rc=data;}
      me.post(wid);
    });
    me.wait(); return rc;
  },
//
  stop: function(id, op){
    var me=this; var ec2=new AWS.EC2(); var pm={}; op=op||{};
    pm=op; pm.InstanceIds=[id]; pm.Force=pm.Force||true;
    var wid=me.ready(); var rc;
    ec2.stopInstances(pm, function(err, data){
      if(err){rc=false; me.error=err;}else{rc=data;}
      me.post(wid);
    });
    me.wait(); return rc;
  },
//ok
  listInstances: function(op){
    var me=this; var ec2=new AWS.EC2(); op=op||{};
    var wid=me.ready(); var rc;
    ec2.describeInstances(op, function(err, data){
      if(err){rc=false; me.error=err;}
      else{
        rc=[]; var a=data.Reservations;
        var k=0; for(var i in a){for(var j in a[i].Instances){rc[k]=a[i].Instances[j]; k++;}}
      }
      me.post(wid);
    });
    me.wait(); return rc;
  },
//ok
  listStatus: function(op){
    var me=this; var ec2=new AWS.EC2(); op=op||{};
    var wid=me.ready(); var rc;
    ec2.describeInstanceStatus(op, function(err, data){
      if(err){rc=false; me.error=err;}else{rc=data;}
      me.post(wi);
    });
    me.wait(); return rc;
  },
//
  snapshot: function(volume, op){
    var me=this; var ec2=new AWS.EC2(); op=op||{}; var pm={};
    pm.VolumeId=volume; pm.Description=op.Description||'Automatic By Nodejs'; 
    var wid=me.ready(); var rc;
    ec2.createSnapshot(pm, function(err, data){
      if(err){rc=false; me.error=err;}else{rc=data;}
      me.post(wid);
    });
    me.wait(); return rc;
  },
//
  onSystemStatusOk: function(ids, proc, op){
    var me=this; var ec2=new AWS.EC2(); op=op||{}; var pm={}; pm=op;
    pm.InstanceIds=ids; 
    ec2.waitFor('systemStatusOk', pm, function(err, data){
      proc(err, data);
    });
  },
//
  onInstanceStatusOk: function(ids, proc, op){
    var me=this; var ec2=new AWS.EC2(); op=op||{}; var pm={}; pm=op;
    pm.InstanceIds=ids; 
    ec2.waitFor('instanceStatusOk', pm, function(err, data){
      proc(err, data);
    });
  },
//
  onSnapshotCompleted: function(proc, pm){
    var me=this; var ec2=new AWS.EC2(); pm=pm||{};
    ec2.waitFor('snapshotCompleted', pm, function(err, data){
      proc(err, data);
    });
  },
//ok
  createSbTable: function(table, op){
    var me=this; var sdb=new AWS.SimpleDB(); var pm={}; pm.DomainName=table;
    var wid=me.ready();
    sdb.createDomain(pm, function(err, data){
      if(err){rc=false; me.error=err;}else{rc=data;}
      me.post(wid);
    });
    me.wait(); return rc;
  },
//ok
  listSbTable: function(pm){
    var me=this; var sdb=new AWS.SimpleDB();
    var wid=me.ready();
    sdb.listDomains(pm, function(err, data){
      if(err){rc=false; me.error=err;}else{rc=data.DomainNames;}
      me.post(wid);
    });
    me.wait(); return rc;
  },
//ok
  deleteSbTable: function(table){
    var me=this; var sdb=new AWS.SimpleDB();
    var pm={}; pm.DomainName=table;
    var wid=me.ready();
    sdb.deleteDomain(pm, function(err, data){
      if(err){rc=false; me.error=err;}else{rc=data;}
      me.post(wid);
    });
    me.wait(); return rc;
  },
//ok
  putSb: function(table, keys){
    var me=this; var sdb=new AWS.SimpleDB(); var pm, rc;
    var k, j; var cnt=0;
    pm={};pm.DomainName=table; var key=''; pm.Attributes=[];

    if(keys){for(k in keys){key+=me.REC[0][k]+'\\';} pm.ItemName=key; j=0;}
    if(!key){
      key=Math.floor(me.now()*100000000+Math.random()*100000000); pm.ItemName=key;
      pm.Attributes[0]={}; pm.Attributes[0]['Name']='_id'; pm.Attributes[0]['Value']=key;
      pm.Attributes[0]['Replace']=true; j=1;
    }
    for(k in me.REC[0]){
      pm.Attributes[j]={};
      pm.Attributes[j]['Name']=k; pm.Attributes[j]['Value']=me.REC[0][k];
      pm.Attributes[j]['Replace']=true; j++;
    }
    var wid=me.ready();
    sdb.putAttributes(pm, function(err, data){
      if(err){rc=false; me.error=err;}else{rc=data;}
      me.post(wid);
    });
    me.wait(); return rc;
  },
//ok
  querySb: function(sql, op){
    var me=this; var sdb=new AWS.SimpleDB(); var pm, rc; op=op||{};
    pm=op; pm.SelectExpression=sql;
    var wid=me.ready();
    sdb.select(pm, function(err, data){
      if(err){rc=false; me.error=err;}
      else{
        me.REC=[]; for(var i in data.Items){
          me.REC[i]={};
          for(var j in data.Items[i].Attributes){
            me.REC[i][data.Items[i].Attributes[j].Name]=data.Items[i].Attributes[j].Value;
          }
        }
        rc=me.REC.length;
      }
      me.post(wid);
    });
    me.wait(); return rc;
  },
//ok
  getSb: function(table, keys, items){
    var me=this; var sdb=new AWS.SimpleDB(); var pm, rc;
    var wid=me.ready(); var k, i;
    pm={}; pm.DomainName=table; var key='';
    for(k in keys){
      if(k=='#'){key=keys[k]; break;}
      key+=keys[k]+'\\';
    }
    pm.ItemName=key; if(items){pm.AttributeNames=items.split(',');}
    pm.ConsistentRead=true;
    me.Table=table; me.Key=key; me.Keys=keys;
    sdb.getAttributes(pm, function(err, data){
      if(err){rc=false; me.error=err;}
      else{
        me.REC=[]; me.REC[0]={}; rc=0; for(var i in data.Attributes){
          me.REC[0][data.Attributes[i].Name]=data.Attributes[i].Value; rc++;
        }
        if(rc==0){me.error='No Data';}
      }
      me.post(wid);
    });
    me.wait(); console.log(rc); return rc;
  },
//ok
  rewriteSb: function(){
    var me=this; var sdb=new AWS.SimpleDB(); var pm, rc;
    return me.putSb(me.Table, me.Keys);
  },
//ok
  deleteSb: function(){
    var me=this; var sdb=new AWS.SimpleDB(); var pm, rc;
    var wid=me.ready();
    pm={};pm.DomainName=me.Table; pm.ItemName=me.Key;
    sdb.deleteAttributes(pm, function(err, data){
      if(err){rc=false; me.error=err;}else{me.REC=[]; rc=true;}
      me.post(wid);
    });
    me.wait(); return rc;
  },
//
  listAlarm: function(pm){
    var me=this; var cw=new AWS.CloudWatch(); var rc; pm=pm||{};
    var wid=me.ready();
    cw.describeAlarms(pm, function(err, data){
      if(err){rc=false; me.error=err;}else{rc=data.MetricAlerms;}
      me.post(wid);
    });
    me.wait(); return rc;
  },
// grunt-rhythm-aws-s3-sync
// toString('base64');
  closeAmazon: function(){
  }
});
module.exports=Amazon;
