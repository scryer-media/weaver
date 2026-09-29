import assert from "node:assert/strict";
import { test } from "node:test";
import { allocation, directLeg, routeInput, routeProblem, routeTargets, adjustWeight, appendLeg, removeLeg } from "../src/lib/networking.ts";
import { policyAsRoute, policyInput } from "../src/lib/proxies.ts";

test("apportionment uses largest remainders with stable positional ties",()=>{
  assert.deepEqual(allocation([60,30,10],50),[30,15,5]);
  assert.deepEqual(allocation([60,30,0],50),[33,17,0]);
  assert.deepEqual(allocation([1,1,1],2),[1,1,0]);
  for(let a=1;a<100;a++)for(let cap=0;cap<20;cap++){
    const targets=allocation([a,100-a],cap);assert.equal(targets.reduce((a,b)=>a+b,0),cap);
  }
});
test("full route serialization preserves pools chains egress and failover without output-only fields",()=>{
  const route={failover:"HOLD" as const,legs:[{egressId:7,weight:100,path:{kind:"LADDER" as const,directFallback:false,rungs:[{kind:"POOL" as const,proxyId:null,poolId:5,chainIds:[]},{kind:"CHAIN" as const,proxyId:null,poolId:null,chainIds:[1,2]}]}}]};
  assert.deepEqual(routeInput(route),{failover:"HOLD",legs:[{egressId:7,weight:100,path:{ladder:{directFallback:false,rungs:[{pool:5},{chain:[1,2]}]}}}]});
  assert.equal(routeProblem(route),null);
});
test("legacy direct and blocked routes preserve their meaning",()=>{
  assert.deepEqual(policyAsRoute({proxyIds:[],allowDirect:true}).legs,[directLeg()]);
  const blocked={proxyIds:[],allowDirect:false};
  assert.equal(policyAsRoute(blocked).legs[0]!.path.kind,"LADDER");
  assert.equal(policyInput(blocked),undefined);
  assert.ok(routeProblem({legs:[{...directLeg(),weight:99}],failover:"REDISTRIBUTE"}));
});

test("health vectors preserve caps and Hold ceilings",()=>{
  for(const weights of [[60,30,10],[34,33,33],[1,99],[100]])for(let cap=0;cap<=50;cap++)for(let mask=0;mask<(1<<weights.length);mask++){
    const legs=weights.map(weight=>({...directLeg(),weight}));
    const down=new Set(weights.flatMap((_,i)=>mask&(1<<i)?[i]:[]));
    const hold=routeTargets({legs,failover:"HOLD"},cap,down), baseline=allocation(weights,cap);
    assert.ok(hold.every((n,i)=>n<=baseline[i]));
    assert.ok(hold.reduce((a,b)=>a+b,0)<=cap);
    const redistributed=routeTargets({legs,failover:"REDISTRIBUTE"},cap,down);
    assert.equal(redistributed.reduce((a,b)=>a+b,0),down.size===weights.length?0:cap);
    for(const i of down){assert.equal(hold[i],0);assert.equal(redistributed[i],0);}
  }
});
test("weight editing adding and removing preserve integer shares",()=>{
  let legs=[directLeg()];
  for(let i=0;i<7;i++)legs=appendLeg(legs);
  assert.equal(legs.length,8);
  for(let i=0;i<legs.length;i++)for(const value of [-20,1,33.3,80,200]){
    legs=adjustWeight(legs,i,value);
    assert.equal(legs.reduce((n,l)=>n+l.weight,0),100);
    assert.ok(legs.every(l=>Number.isInteger(l.weight)&&l.weight>0));
  }
  while(legs.length>1)legs=removeLeg(legs,legs.length-1);
  assert.equal(legs[0].weight,100);
});
