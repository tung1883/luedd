document.addEventListener('DOMContentLoaded', function () {
    window.setTimeout(()=>{
        document.getElementById("link").click();
    },1000);
    document.getElementById("link").href = "xdm-app:" + chrome.runtime.getURL("/");
}, false);