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
test("flow displays route evidence, opens editors, and edits weights without losing capacity",async()=>{
 // Tall enough to hold the whole fixture: the page itself does not scroll.
 const page=await browser.newPage({viewport:{width:1440,height:2700}});page.setDefaultTimeout(0);const errors=[];page.on("pageerror",error=>errors.push(error.message));
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
 // The member carrying the pool's traffic says so, with what it carries now; the one idle beside it stands by on its timings.
 await members.getByText("Amsterdam · Active",{exact:true}).waitFor();
 // The pool lists Frankfurt, Lisbon, Amsterdam; it is drawn by what carries traffic, then by the quickest to connect.
 assert.deepEqual(await members.locator("text").allTextContents(),["Amsterdam · Active","24 open · 22.89 MiB/s","Frankfurt · Standby","25 ms · 0.76 MiB/s each","Lisbon · Disabled"]);
 // A connect time wears the signal mark instead of words; the hover, which has no room for it, says what it is.
 assert.equal(await members.locator("a").filter({hasText:"Frankfurt · Standby"}).locator("svg.lucide-signal").count(),1);
 assert.equal(await members.locator("svg.lucide-signal").count(),1);
 assert.equal(await members.locator("a").filter({hasText:"Amsterdam · Active"}).locator("title").textContent(),"Connects in 20 ms · 0.95 MiB/s each");
 assert.equal(await members.locator("rect[fill='#1f2b29']").count(),1);
 const pool=diagram.locator("a").filter({hasText:"Europe · pool"});
 assert.deepEqual(await pool.locator("text").allTextContents(),["Europe · pool","24 connections","Active"]);
 const chain=diagram.getByRole("group",{name:"Relay one > Relay two > Exit",exact:true});
 assert.deepEqual(await chain.locator("text").allTextContents(),["Relay one","Up",">","Relay two","Failing","The connection was","refused. Check the","port and that the","proxy is running.",">","Exit","Unreached"]);
 await diagram.getByText("Direct · Active",{exact:true}).first().waitFor();
 const proxy=diagram.locator("a").filter({hasText:"Frankfurt · WireGuard"});
 assert.deepEqual(await proxy.locator("text").allTextContents(),["Frankfurt · WireGuard","Failing","No handshake within 12 seconds. Check the endpoint, and that the","server's peer list has public key Fixture+peer+key=."]);
 await diagram.locator("a").filter({hasText:"Amsterdam · WireGuard"}).getByText("Active",{exact:true}).waitFor();
 assert.equal(await diagram.locator("a[href='/settings/networking/proxies']").count(),9);
 // A server behind a kill switch has no rung and may not go direct: the kill switch is the box its line stops at.
 const killSwitch=diagram.getByRole("link",{name:"Set a route for News archive",exact:true});
 assert.deepEqual(await killSwitch.locator("text").allTextContents(),["Kill switch","Blocked · no proxy, and direct is off"]);
 // Every box wears its state down its left edge: egresses, legs, endpoints, chain hops, lone proxies, the pool's heading and card, and the kill switch.
 assert.equal(await diagram.locator("rect[width='3']").count(),3+7+5+3+2+2+1);
 const legCards=diagram.locator("a[aria-label^='Edit the route for']");
 assert.deepEqual(await legCards.filter({hasText:"leg 3"}).locator("text").allTextContents(),["News primary · leg 3 · 10%","Down · 0/0 open · 0.00 MiB/s","parked · hold","Interface is down"]);
 // An egress says each limit it is held to under its load, speed then quota, a row each on a card that grows to fit.
 const fiber=diagram.locator("a[href='/settings/networking/egress']").filter({hasText:"WAN Fiber"});
 assert.deepEqual(await fiber.locator("text").allTextContents(),["WAN Fiber","Up","192.0.2.10","24 open · 22.89 MiB/s","50.00 MiB/s","120 GB / 500 GB"]);
 assert.deepEqual(await fiber.locator("g:has(> title) svg").evaluateAll(icons=>icons.map(icon=>icon.getAttribute("class").split(" ").find(name=>name!=="lucide"))),["lucide-gauge","lucide-chart-pie"]);
 assert.deepEqual(await fiber.locator("g:has(> title) > title").allTextContents(),["Download limit","Download quota"]);
 // An egress leaving from more than one address stacks them, one to a line, and its card grows to hold them.
 const system=diagram.locator("a[href='/settings/networking/egress']").filter({hasText:"System"});
 const systemTexts=await system.locator("text").evaluateAll(texts=>texts.map(text=>[text.textContent,Number(text.getAttribute("y"))]));
 assert.deepEqual(systemTexts.map(([text])=>text).slice(0,4),["System","Up","192.0.2.21","2001:db8::21"]);
 assert.deepEqual(systemTexts.slice(2,5).map(([,y],index,all)=>index?y-all[index-1][1]:0),[0,14,20]);
 assert.deepEqual(await diagram.locator("a[href='/settings/networking/egress'] > rect:first-child").evaluateAll(rects=>rects.map(rect=>rect.getAttribute("height"))),["122","148","128"]);
 const lte=diagram.locator("a[href='/settings/networking/egress']").filter({hasText:"LTE standby"});
 assert.deepEqual(await lte.locator("text").allTextContents(),["LTE standby","Down","198.51.100.10","Interface is down","20.0 GB / 20.0 GB · Blocked"]);
 assert.notEqual(await lte.locator("g:has(> title) > text").getAttribute("fill"),await fiber.locator("g:has(> title) > text").last().getAttribute("fill"));
 // The daemon knows only that its egress is up; the leg is blocked all the same.
 assert.deepEqual(await legCards.filter({hasText:"News archive"}).locator("text").allTextContents(),["News archive · leg 1 · 100%","Blocked · 0/0 open · 0.00 MiB/s","0 planned"]);
 await diagram.locator("path[stroke-dasharray]:not([stroke-dasharray='18 30'])").nth(10).waitFor({state:"attached"});
 assert.equal(await diagram.locator("path[stroke-dasharray]:not([stroke-dasharray='18 30'])").count(),11);
 const faded=diagram.locator("[opacity='0.2']");
 const fadedLegs=diagram.locator("g[opacity='0.2'] > a[aria-label^='Edit the route for']");
 const fadedEgresses=diagram.locator("g[opacity='0.2'] > a[href='/settings/networking/egress']");
 const endpoints=diagram.locator("a[href^='/settings/networking/routes']:not([aria-label])");
 const fadedEndpoints=diagram.locator("g[opacity='0.2'] > a[href^='/settings/networking/routes']:not([aria-label])");
 assert.equal(await faded.count(),0);
 // Only a server's card counts its connections: a feed's is its name over what it is, Default outbound's its name alone.
 for(const [name,texts] of [["Feed mirror",["Feed mirror","RSS feed"]],["Default outbound",["Default outbound"]]]){
  const card=endpoints.filter({hasText:name});
  assert.deepEqual(await card.locator("text").allTextContents(),texts);
  assert.equal(await card.locator("rect").count(),2);
 }
 // A feed's card carries the RSS mark; no other card does.
 assert.equal(await endpoints.filter({hasText:"Feed mirror"}).locator("svg.lucide-rss").count(),1);
 assert.equal(await diagram.locator("svg.lucide-rss").count(),1);
 // A server's card keeps a box for each of its 50 connections.
 assert.equal(await endpoints.filter({hasText:"News primary"}).locator("rect").count(),2+50);
 // At rest only the legs moving bytes move: News primary's first and News backup's, three lines each.
 // Pointed at a box, only what it lights does.
 const snakes=diagram.locator("animate");
 const fadedSnakes=diagram.locator("g[opacity='0.2'] animate");
 // Every snake runs in a ribbon of its own green; a dashed thread never carries one.
 // The snakes are dashed too; these are the threads.
 const dashed=diagram.locator("path[stroke-dasharray]:not([stroke-dasharray='18 30'])");
 const ribbons=diagram.locator("g:has(> path > animate) > path:not([fill='none'])");
 await snakes.nth(5).waitFor({state:"attached"});
 assert.equal(await snakes.count(),6);
 assert.equal(await ribbons.count(),6);
 // The pool's traffic goes in and out at the member carrying it, not at the pool's head.
 const carrier=members.locator("rect[fill='#1f2b29']");
 const carrierMiddle=Number(await carrier.getAttribute("y"))+Number(await carrier.getAttribute("height"))/2;
 const snakePaths=await snakes.evaluateAll(nodes=>nodes.map(node=>node.parentElement.getAttribute("d").trim().split(/\s+/).map(Number)));
 assert.ok(snakePaths.some(d=>Math.abs(d.at(-1)-carrierMiddle)<0.01),"a snake ends at the carrying member");
 assert.ok(snakePaths.some(d=>Math.abs(d[2]-carrierMiddle)<0.01),"a snake starts at the carrying member");
 await chain.hover();
 await fadedEndpoints.filter({hasText:"News primary"}).waitFor({state:"attached"});
 assert.deepEqual(await fadedLegs.evaluateAll(legs=>legs.map(leg=>leg.getAttribute("aria-label"))),["Edit the route for Default outbound, leg 1","Edit the route for News primary, leg 2","Edit the route for News backup, leg 1","Edit the route for News archive, leg 1","Edit the route for News primary, leg 1","Edit the route for News primary, leg 3"]);
 assert.equal(await chain.getAttribute("opacity"),null);
 assert.equal(await fadedSnakes.count(),0);
 assert.ok(await snakes.count()>0);
 assert.equal(await ribbons.count(),await snakes.count());
 assert.equal(await ribbons.evaluateAll(paths=>paths.filter(path=>path.getAttribute("fill")!==path.parentElement.querySelector(":scope > path:has(> animate)").getAttribute("stroke")).length),0);
 assert.equal(await proxy.locator("xpath=..").getAttribute("opacity"),"0.2");
 assert.equal(await killSwitch.locator("xpath=..").getAttribute("opacity"),"0.2");
 assert.equal(await fadedEgresses.count(),2);
 assert.equal(await fadedEndpoints.count(),3);
 // An endpoint nothing reaches is already dimmed, and fades further.
 const archive=endpoints.filter({hasText:"News archive"}).locator("xpath=..");
 assert.ok(Number(await archive.getAttribute("opacity"))<0.2);
 if(process.env.NETWORKING_SCREENSHOT)await page.screenshot({path:process.env.NETWORKING_SCREENSHOT.replace(/\.png$/,"-focus-chain.png"),fullPage:true});
 await diagram.locator("a[href='/settings/networking/egress']").filter({hasText:"WAN Fiber"}).hover();
 await fadedEndpoints.filter({hasText:"Feed mirror"}).waitFor({state:"attached"});
 assert.deepEqual(await fadedLegs.evaluateAll(legs=>legs.map(leg=>leg.getAttribute("aria-label"))),["Edit the route for Default outbound, leg 1","Edit the route for Feed mirror, leg 1","Edit the route for News primary, leg 2","Edit the route for News backup, leg 1","Edit the route for News archive, leg 1","Edit the route for News primary, leg 3"]);
 assert.ok(await snakes.count()>0);
 assert.equal(await fadedSnakes.count(),0);
 assert.equal(await ribbons.count(),await snakes.count());
 assert.equal(await ribbons.evaluateAll(paths=>paths.filter(path=>path.getAttribute("fill")!==path.parentElement.querySelector(":scope > path:has(> animate)").getAttribute("stroke")).length),0);
 assert.equal(await fadedEgresses.count(),2);
 assert.equal(await fadedEndpoints.count(),3);
 assert.ok(Number(await archive.getAttribute("opacity"))<0.2);
 if(process.env.NETWORKING_SCREENSHOT)await page.screenshot({path:process.env.NETWORKING_SCREENSHOT.replace(/\.png$/,"-focus-egress.png"),fullPage:true});
 // The highlight follows the pointer and nothing else: it is gone once the pointer is.
 await page.mouse.move(0,0);
 await faded.first().waitFor({state:"detached"});
 assert.equal(await snakes.count(),6);
 assert.equal(await dashed.count(),11);
 // A route behind a kill switch lights up but does not move: nothing can go that way.
 await killSwitch.hover();
 await fadedEndpoints.filter({hasText:"News primary"}).waitFor({state:"attached"});
 assert.equal(await killSwitch.locator("xpath=..").getAttribute("opacity"),null);
 assert.equal(await snakes.count(),0);
 assert.equal(await dashed.count(),11);
 await page.mouse.move(0,0);
 await faded.first().waitFor({state:"detached"});
 // A reader who asks for less motion gets none, from the moment they ask: the traffic stands still in its ribbons.
 await page.emulateMedia({reducedMotion:"reduce"});
 await snakes.first().waitFor({state:"detached"});
 assert.equal(await diagram.locator("path[stroke-dasharray='18 30']").count(),6);
 await page.emulateMedia({reducedMotion:"no-preference"});
 await snakes.nth(5).waitFor({state:"attached"});
 // Picking a box opens the editor for what the box stands for, and holds no highlight.
 const edits=[
  [diagram.getByRole("link",{name:"Edit the route for Feed mirror, leg 1",exact:true}),"Route for Feed mirror"],
  [endpoints.filter({hasText:"News backup"}),"Route for News backup"],
  [endpoints.filter({hasText:"Default outbound"}),"Route for Default outbound"],
  [killSwitch,"Route for News archive"],
  [diagram.locator("a[href='/settings/networking/egress']").filter({hasText:"WAN Fiber"}),"WAN Fiber"],
  [pool,"Europe"],
  [members.locator("a").filter({hasText:"Frankfurt · Standby"}),"Frankfurt"],
  [chain.locator("a").filter({hasText:"Relay two"}),"Relay two"],
 ];
 for(const [box,title] of edits){
  await box.click();
  const editor=page.getByRole("dialog",{name:title,exact:true});
  await editor.waitFor();
  if(title==="Europe"&&process.env.NETWORKING_SCREENSHOT)await page.screenshot({path:process.env.NETWORKING_SCREENSHOT.replace(/\.png$/,"-edit.png"),fullPage:true});
  if(title==="Route for News archive"&&process.env.NETWORKING_SCREENSHOT)await editor.screenshot({path:process.env.NETWORKING_SCREENSHOT.replace(/\.png$/,"-edit-kill-switch.png")});
  await page.keyboard.press("Escape");
  await editor.waitFor({state:"detached"});
  await page.mouse.move(0,0);
  await faded.first().waitFor({state:"detached"});
 }
 if(process.env.NETWORKING_SCREENSHOT)await page.screenshot({path:process.env.NETWORKING_SCREENSHOT.replace(/\.png$/,"-live.png"),fullPage:true});
 // The flow is not offered as a file.
 assert.equal(await page.getByRole("button",{name:"Download SVG"}).count(),0);
 const slider=page.getByRole("slider").first();await slider.focus();await page.keyboard.press("ArrowRight");
 await page.getByLabel("Route weights").filter({hasText:"61,29,10"}).waitFor();
 // A rung may only name a pool with a member in it: the empty one is not offered.
 await page.getByRole("button",{name:"Leg 1 rung 1",exact:true}).click();
 const offered=await page.getByRole("menu",{name:"Leg 1 rung 1",exact:true}).getByRole("menuitemradio").allTextContents();
 assert.ok(offered.includes("Europe")&&offered.includes("Relays"),offered.join(","));
 assert.ok(!offered.includes("Empty"),offered.join(","));
 await page.keyboard.press("Escape");
 await page.getByRole("menu",{name:"Leg 1 rung 1",exact:true}).waitFor({state:"detached"});
 await page.getByRole("button",{name:"Toggle all down"}).click();
 await page.getByText("0 / 50",{exact:true}).first().waitFor();
 await dashed.nth(15).waitFor({state:"attached"});
 assert.equal(await dashed.count(),16);
 // With one route picked, only what it reaches is drawn: its legs, the egress they leave through and its own card.
 const egressCards=diagram.locator("a[href='/settings/networking/egress']");
 await page.getByRole("button",{name:"Endpoint",exact:true}).click();
 await page.getByRole("menuitemradio",{name:"Feed mirror",exact:true}).click();
 await egressCards.filter({hasText:"WAN Fiber"}).waitFor({state:"detached"});
 assert.equal(await egressCards.count(),1);
 await egressCards.filter({hasText:"System"}).waitFor();
 assert.deepEqual(await endpoints.allTextContents(),["Feed mirrorRSS feed"]);
 assert.deepEqual(await legCards.evaluateAll(legs=>legs.map(leg=>leg.getAttribute("aria-label"))),["Edit the route for Feed mirror, leg 1"]);
 assert.equal(await diagram.locator("a").filter({hasText:"Europe · pool"}).count(),0);
 if(process.env.NETWORKING_SCREENSHOT)await page.screenshot({path:process.env.NETWORKING_SCREENSHOT.replace(/\.png$/,"-one-route.png"),fullPage:true});
 await page.getByRole("button",{name:"Endpoint",exact:true}).click();
 await page.getByRole("menuitemradio",{name:"All endpoints",exact:true}).click();
 await egressCards.filter({hasText:"WAN Fiber"}).waitFor();
 assert.equal(await endpoints.count(),5);
 assert.equal(errors.length,0,errors.join("\n"));
 if(process.env.NETWORKING_SCREENSHOT)await page.screenshot({path:process.env.NETWORKING_SCREENSHOT,fullPage:true});
 await page.close();
});
