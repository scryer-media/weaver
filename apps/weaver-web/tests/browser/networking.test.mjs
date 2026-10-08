import assert from "node:assert/strict";
import { after, before, test } from "node:test";
import { createServer } from "vite";
const { chromium } = await import(process.env.PLAYWRIGHT_MODULE_PATH ?? "playwright");
let server, browser, baseUrl;
before(async()=>{server=await createServer({cacheDir:"node_modules/.vite/browser-networking",server:{host:"127.0.0.1",port:0,...(process.env.NETWORKING_FIXTURE_API?{proxy:{"/graphql":{target:process.env.NETWORKING_FIXTURE_API,ws:true}}}:{})}});await server.listen();baseUrl=`http://127.0.0.1:${server.httpServer.address().port}`;browser=await chromium.launch({headless:true});});
after(async()=>{await browser?.close();await server?.close();});
test("create an egress and two-leg route, then disable the egress through GraphQL",{skip:!process.env.NETWORKING_FIXTURE_API},async()=>{
 const page=await browser.newPage({viewport:{width:1440,height:1100}});page.setDefaultTimeout(0);
 const errors=[];page.on("pageerror",error=>errors.push(error.message));
 await page.goto(`${baseUrl}/tests/browser/networking-api.html`);
 console.log("Networking fixture loaded");
 await page.getByRole("button",{name:"Add egress",exact:true}).click();
 await page.getByLabel("Name",{exact:true}).fill("Fixture WAN");
 await page.getByRole("button",{name:"Binding",exact:true}).click();
 await page.getByRole("menuitemradio",{name:"Source address",exact:true}).click();
 await page.getByRole("combobox",{name:"Source address",exact:true}).fill("127.0.0.1");
 await page.getByRole("button",{name:"Save egress",exact:true}).click();
 await page.getByRole("button",{name:/^Fixture WAN/}).waitFor();
 console.log("Created egress through the editor");
 await page.getByRole("link",{name:"Routes",exact:true}).click();
 await page.getByRole("button",{name:"Edit route",exact:true}).click();
 await page.getByRole("button",{name:"Add leg",exact:true}).click();
 await page.getByRole("group",{name:"Leg 2",exact:true}).getByRole("button",{name:"Egress",exact:true}).click();
 await page.getByRole("menuitemradio",{name:"Fixture WAN",exact:true}).click();
 await page.getByRole("button",{name:"Save route",exact:true}).click();
 await page.getByText("20 NNTP connections · 2 legs",{exact:true}).waitFor();
 console.log("Saved two-leg route");
 await page.getByRole("link",{name:"Overview",exact:true}).click();
 await page.getByRole("link",{name:"Edit the route for news.fixture.invalid, leg 2",exact:true}).waitFor();
 assert.equal(await page.locator("svg").getByRole("link",{name:/Edit the route for .* leg/}).count(),2);
 assert.equal(await page.locator("svg path[stroke-dasharray]").count(),4);
 const gql=async(query,variables={})=>{const r=await page.request.post(`${baseUrl}/graphql`,{data:{query,variables}});const body=await r.json();assert.equal(body.errors,undefined,JSON.stringify(body.errors));return body.data;};
 const data=await gql("{egressInterfaces{id name} networkFlow{legs{position target state}}}");
 assert.deepEqual(data.networkFlow.legs.map(l=>l.target),[10,10]);
 const id=data.egressInterfaces.find(e=>e.name==="Fixture WAN").id;
 await gql("mutation($id:Int!){updateEgressInterface(id:$id,input:{name:\"Fixture WAN\",bindingKind:SOURCE_ADDRESS,sourceAddress:\"127.0.0.1\",enabled:false}){id}}",{id});
 await page.getByText("share moved",{exact:true}).waitFor();
 const after=await gql("{networkFlow{legs{position target state}}}");
 assert.deepEqual(after.networkFlow.legs.map(l=>l.target),[20,0]);
 assert.equal(after.networkFlow.legs[1].state,"DOWN");
 await page.locator("svg a").filter({hasText:"Fixture WAN"}).getByText("Down",{exact:true}).waitFor();
 assert.equal(errors.length,0,errors.join("\n"));
 if(process.env.NETWORKING_SCREENSHOT)await page.screenshot({path:process.env.NETWORKING_SCREENSHOT.replace(/\.png$/,"-api.png"),fullPage:true});
 await page.close();
});
test("flow displays route evidence, opens editors, exports a frozen SVG, and edits weights without losing capacity",async()=>{
 // Tall enough to hold the whole fixture: the page itself does not scroll.
 const page=await browser.newPage({viewport:{width:1440,height:2300}});page.setDefaultTimeout(0);const errors=[];page.on("pageerror",error=>errors.push(error.message));
 await page.goto(`${baseUrl}/tests/browser/networking.html`);
 const diagram=page.locator("svg[aria-label^='Egress interfaces']").first();
 const lanes=["EGRESS INTERFACES","ROUTE LEGS","PROXIES","ENDPOINTS"];
 for(const lane of lanes)await diagram.getByText(lane,{exact:true}).waitFor();
 const laneX=await Promise.all(lanes.map(async lane=>Number(await diagram.getByText(lane,{exact:true}).getAttribute("x"))));
 assert.deepEqual(laneX,[...laneX].sort((a,b)=>a-b));
 assert.equal(await diagram.getByText(/consumer/i).count(),0);
 await page.getByRole("button",{name:"Endpoint",exact:true}).waitFor();
 assert.equal(await diagram.locator("a[href='/settings/networking/egress']").count(),3);
 assert.equal(await diagram.getByText("Spare uplink",{exact:true}).count(),0);
 const members=diagram.getByRole("group",{name:"Europe pool members",exact:true});
 await members.getByText("Amsterdam · Pinned",{exact:true}).waitFor();
 assert.deepEqual(await members.locator("text").allTextContents(),["Amsterdam · Pinned","Session 30 ms · open 20 ms · 0.95 MiB/s","Frankfurt · Ready","Session 40 ms · open 25 ms · 0.76 MiB/s","Lisbon · Disabled"]);
 const pool=diagram.locator("a").filter({hasText:"Europe · pool"});
 assert.deepEqual(await pool.locator("text").allTextContents(),["Europe · pool","24 connections","Active · pinned to Amsterdam"]);
 const chain=diagram.getByRole("group",{name:"Relay one > Relay two > Exit",exact:true});
 assert.deepEqual(await chain.locator("text").allTextContents(),["Relay one","Up",">","Relay two","Failing","endpoint","unreachable:","connection refused",">","Exit","Unreached"]);
 await diagram.getByText("Direct · no tunnel · Active",{exact:true}).first().waitFor();
 const proxy=diagram.locator("a").filter({hasText:"Frankfurt · WireGuard"});
 assert.deepEqual(await proxy.locator("text").allTextContents(),["Frankfurt · WireGuard","Failing","handshake did not complete"]);
 await diagram.locator("a").filter({hasText:"Amsterdam · WireGuard"}).getByText("Active",{exact:true}).waitFor();
 assert.equal(await diagram.locator("a[href='/settings/networking/proxies']").count(),9);
 // Every box wears its state down its left edge: egresses, legs, endpoints, chain hops, lone proxies, and the pool's heading and card.
 assert.equal(await diagram.locator("rect[width='3']").count(),3+5+3+3+2+2);
 const legCards=diagram.locator("a[aria-label^='Edit the route for']");
 assert.deepEqual(await legCards.filter({hasText:"leg 3"}).locator("text").allTextContents(),["News primary · leg 3 · 10%","Down · 0/0 open · 0.00 MiB/s","parked · hold","Interface is down"]);
 assert.deepEqual(await diagram.locator("a[href='/settings/networking/egress']").filter({hasText:"LTE standby"}).locator("text").allTextContents(),["LTE standby","Down","198.51.100.10","Interface is down"]);
 await diagram.locator("path[stroke-dasharray]").nth(5).waitFor({state:"attached"});
 assert.equal(await diagram.locator("path[stroke-dasharray]").count(),6);
 const faded=diagram.locator("[opacity='0.2']");
 const fadedLegs=diagram.locator("g[opacity='0.2'] > a[aria-label^='Edit the route for']");
 const fadedEgresses=diagram.locator("g[opacity='0.2'] > a[href='/settings/networking/egress']");
 const endpoints=diagram.locator("a[href^='/settings/networking/routes']:not([aria-label])");
 const fadedEndpoints=diagram.locator("g[opacity='0.2'] > a[href^='/settings/networking/routes']:not([aria-label])");
 assert.equal(await faded.count(),0);
 await chain.hover();
 await fadedEndpoints.filter({hasText:"News primary"}).waitFor({state:"attached"});
 assert.deepEqual(await fadedLegs.evaluateAll(legs=>legs.map(leg=>leg.getAttribute("aria-label"))),["Edit the route for News primary, leg 2","Edit the route for News backup, leg 1","Edit the route for News primary, leg 1","Edit the route for News primary, leg 3"]);
 assert.equal(await chain.getAttribute("opacity"),null);
 assert.equal(await proxy.locator("xpath=..").getAttribute("opacity"),"0.2");
 assert.equal(await fadedEgresses.count(),2);
 assert.equal(await fadedEndpoints.count(),2);
 if(process.env.NETWORKING_SCREENSHOT)await page.screenshot({path:process.env.NETWORKING_SCREENSHOT.replace(/\.png$/,"-focus-chain.png"),fullPage:true});
 await diagram.locator("a[href='/settings/networking/egress']").filter({hasText:"WAN Fiber"}).hover();
 await fadedEndpoints.filter({hasText:"Feed mirror"}).waitFor({state:"attached"});
 assert.deepEqual(await fadedLegs.evaluateAll(legs=>legs.map(leg=>leg.getAttribute("aria-label"))),["Edit the route for Feed mirror, leg 1","Edit the route for News primary, leg 2","Edit the route for News backup, leg 1","Edit the route for News primary, leg 3"]);
 assert.equal(await fadedEgresses.count(),2);
 assert.equal(await fadedEndpoints.count(),2);
 if(process.env.NETWORKING_SCREENSHOT)await page.screenshot({path:process.env.NETWORKING_SCREENSHOT.replace(/\.png$/,"-focus-egress.png"),fullPage:true});
 // The highlight follows the pointer and nothing else: it is gone once the pointer is.
 await page.mouse.move(0,0);
 await faded.first().waitFor({state:"detached"});
 // Picking a box opens the editor for what the box stands for, and holds no highlight.
 const edits=[
  [diagram.getByRole("link",{name:"Edit the route for Feed mirror, leg 1",exact:true}),"Route for Feed mirror"],
  [endpoints.filter({hasText:"News backup"}),"Route for News backup"],
  [diagram.locator("a[href='/settings/networking/egress']").filter({hasText:"WAN Fiber"}),"WAN Fiber"],
  [pool,"Europe"],
  [members.locator("a").filter({hasText:"Frankfurt · Ready"}),"Frankfurt"],
  [chain.locator("a").filter({hasText:"Relay two"}),"Relay two"],
 ];
 for(const [box,title] of edits){
  await box.click();
  const editor=page.getByRole("dialog",{name:title,exact:true});
  await editor.waitFor();
  if(title==="Europe"&&process.env.NETWORKING_SCREENSHOT)await page.screenshot({path:process.env.NETWORKING_SCREENSHOT.replace(/\.png$/,"-edit.png"),fullPage:true});
  await page.keyboard.press("Escape");
  await editor.waitFor({state:"detached"});
  await page.mouse.move(0,0);
  await faded.first().waitFor({state:"detached"});
 }
 // A server's own editor shows its route and does not edit it: three lanes, no links, no controls.
 const own=page.getByRole("region",{name:"Server editor route",exact:true});
 for(const lane of lanes.slice(0,3))await own.getByText(lane,{exact:true}).waitFor();
 assert.equal(await own.getByText("ENDPOINTS",{exact:true}).count(),0);
 for(const leg of ["Leg 1 · 60%","Leg 2 · 30%","Leg 3 · 10%"])await own.getByText(leg,{exact:true}).waitFor();
 await own.getByText("Europe · pool",{exact:true}).waitFor();
 assert.equal(await own.locator("svg a").count(),0);
 assert.equal(await own.getByRole("button").count(),0);
 assert.equal(await own.getByRole("link",{name:"Edit in Networking",exact:true}).getAttribute("href"),"/settings/networking/routes?consumer=server%3A1");
 const unsaved=page.getByRole("region",{name:"New server route",exact:true});
 await unsaved.getByText(/^Starts on the direct route/).waitFor();
 assert.equal(await unsaved.locator("svg").count(),0);
 assert.equal(await unsaved.getByRole("link").count(),0);
 if(process.env.NETWORKING_SCREENSHOT)await own.screenshot({path:process.env.NETWORKING_SCREENSHOT.replace(/\.png$/,"-server-route.png")});
 if(process.env.NETWORKING_SCREENSHOT)await page.screenshot({path:process.env.NETWORKING_SCREENSHOT.replace(/\.png$/,"-live.png"),fullPage:true});
 const [download]=await Promise.all([page.waitForEvent("download"),page.getByRole("button",{name:"Download SVG"}).click()]);
 const stream=await download.createReadStream();let exported="";for await(const chunk of stream)exported+=chunk;
 assert.match(exported,/192\.0\.2\.10/);assert.match(exported,/2026-/);assert.match(exported,/<path/);assert.match(exported,/Relay two/);assert.match(exported,/ENDPOINTS/);assert.doesNotMatch(exported,/password|privateKey|credentials/);
 const slider=page.getByRole("slider").first();await slider.focus();await page.keyboard.press("ArrowRight");
 await page.getByLabel("Route weights").filter({hasText:"61,29,10"}).waitFor();
 await page.getByRole("button",{name:"Toggle all down"}).click();
 await page.getByText("0 / 50",{exact:true}).first().waitFor();
 await diagram.locator("path[stroke-dasharray]").nth(10).waitFor({state:"attached"});
 assert.equal(await diagram.locator("path[stroke-dasharray]").count(),11);
 assert.equal(errors.length,0,errors.join("\n"));
 if(process.env.NETWORKING_SCREENSHOT)await page.screenshot({path:process.env.NETWORKING_SCREENSHOT,fullPage:true});
 await page.close();
});
