var SCRIPT_NAME = 'asq_container.js'; 
var scripts = document.getElementsByTagName("script"); 
var script = null; 
var i = scripts.length-1; 
while (i >= 0) {
	if (scripts[i].src.search(SCRIPT_NAME) >= 0) {
		script = scripts[i];
		break;
	}
	i--;
}
function getUrlVars() {
	var vars = [];
	var hash;
	try {
		var hashes = script.src.slice(script.src.indexOf('#') + 1).split('&');
		for (var i = 0; i < hashes.length; i++) {
			hash = hashes[i].split('=');
			vars.push(hash[0]);
			vars[hash[0]] = hashes[i].slice(hashes[i].indexOf('=') + 1);
		}
	} catch (e) {}
	return vars;
}
function addTag(tag, src, dom) {
	var tag = document.createElement(tag);
	tag.setAttribute("src", src);
	tag.setAttribute("width", "1");
	tag.setAttribute("height", "1");
	dom.parentNode.appendChild(tag);
}
if (script) {
	var qs = getUrlVars();
	var alephd_src = 
'http://pixel.alephd.com/post_asq?LgDc='+qs.v+'&L4DK='+qs.b+'&rZzC='+qs.c+'&aNVl='+qs.d+'&ZzpC='+qs.e+'&Dsph='+qs.f+'&Hsyz='+qs.g+'&0ZGY='+qs.h+'&Xyes='+qs.i+'&iK5S='+qs.j+'&b6lE='+qs.k+'&-tgi='+qs.l+'&wziz='+qs.m+'&vKcZ='+qs.n+'&2jSB='+qs.o+'&-Zu8='+qs.p+'&e9iH='+qs.q+'&b5ho='+qs.r+'&GAa8='+qs.a+'&DH5k='+qs.s+'&8rSZ='+qs.t+'&-Z06='+qs.u+'&m9kT=img';
	var moat_src = 
'http://js.moatads.com/audiencesquareappnexus107864036450/moatad.js#moatClientLevel1='+qs.x+'&moatClientLevel2='+qs.y+'&moatClientLevel3='+qs.z+'&moatClientLevel4='+qs.h+'&moatClientSlicer1='+qs.a1;
	//var forensiq_src = 'http://c.fqtag.com/tag/implement.js?org=prazusW5kesewanapreg&fmt=banner&s='+qs.n+'&rd='+qs.a1+'&p=1608&a='+qs.f+'&cmp='+qs.z+'&c1='+qs.b+'&c2='+qs.h+'&c3='+qs.q+'&c4='+qs.a+'&ctu=&rt=display&sl=1&apn=1';
	addTag('img', alephd_src, script);
	//addTag('script', forensiq_src, script);
	//addTag('script', moat_src, script);
	// nugg.ad pixel
	if(typeof window.top.nuggad_on === "undefined" && typeof qs.w != "undefined")
		addTag('script', '//'+decodeURIComponent(qs.w)+'&nuggrid=' + encodeURIComponent(top.location.href), script);
	if(typeof qs.width != "undefined" && typeof qs.height != "undefined" && window.frameElement) {
		if (typeof(asq_resize) == 'undefined' || (typeof(asq_resize) == 'object' && typeof(asq_resize[qs.a]) == 'undefined')) {
			window.frameElement.style.width = qs.width+'px'; 
			window.frameElement.style.height = qs.height+'px';
			window.frameElement.width = qs.width; 
			window.frameElement.height = qs.height;
		}
	}
	if(typeof qs.segments!='undefined') {
		var segments = qs.segments.split(',');
		for (i=0;i<segments.length;i++)
			addTag('img', (window.location.protocol == 'http:' ? 'http://ib': 'https://secure')+'.adnxs.com/seg?add='+segments[i]+'&t=2', script);
	}
}
function getAsqSizes() {
	return {'width': qs.width, 'height': qs.height};
}

