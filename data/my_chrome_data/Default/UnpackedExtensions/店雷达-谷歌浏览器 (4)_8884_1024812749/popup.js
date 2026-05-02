// eslint-disable-next-line no-unused-vars
let tabArr = [];
let y = new Date().getFullYear();
let m = new Date().getMonth() < 9 ? "0" + (new Date().getMonth() + 1) : new Date().getMonth() + 1;
let d = new Date().getDate() < 10 ? "0" + new Date().getDate() : new Date().getDate();
// eslint-disable-next-line no-unused-vars
let t = `${y}${m}${d}`;
let userInfo = null;
// eslint-disable-next-line no-unused-vars
let isDev = false;
let isTest = false;
let base = "https://api.dianleida.net/dld/";
let admin = "https://www.dianleida.net/";

const md = function(functionName, source) {
  fetch(base + "api/BuryingPoint/InfoRecord/Add", {
    method: "post",
    headers: {
      "Content-Type": "application/json",
      "token": userInfo?.token
    },
    body: JSON.stringify({
      functionName,
      source
    })
  });
};
const getuserInfo = function() {
  chrome.cookies.get(
    {
      url: admin,
      name: "dld_user"
    },
    function(cookies) {
      if (cookies) {
        userInfo = JSON.parse(unescape(cookies.value));
      }
    }
  );
};

let list = {
  tab1: [
    {
      href: "https://www.dianleida.net/",
      img: "https://obs.dianleida.net/public/popup/guanwang.png",
      text: "店雷达官网",
      functionName: "导航栏_主流平台【店雷达官网】"
    },
    {
      href: "https://www.1688.com/",
      img: "https://obs.dianleida.net/public/popup/1688.png",
      text: "1688",
      functionName: "导航栏_主流平台【1688】"
    },
    {
      href: "https://www.alibaba.com/",
      img: "https://obs.dianleida.net/public/popup/alibaba.png",
      text: "阿里国际",
      functionName: "导航栏_主流平台【阿里国际】"
    },
    {
      href: "https://pifa.pinduoduo.com",
      img: "https://obs.dianleida.net/public/popup/pinduoduopifa.png",
      text: "拼多多批发",
      functionName: "导航栏_跨境平台【拼多多批发】"
    },
    {
      href: "https://www.amazon.com/",
      img: "https://obs.dianleida.net/public/popup/yamaxun.png",
      text: "亚马逊",
      functionName: "导航栏_主流平台【亚马逊】"
    },
    {
      href: "https://www.aliexpress.com",
      img: "https://obs.dianleida.net/public/popup/sumaitong.png",
      text: "速卖通",
      functionName: "导航栏_主流平台【速卖通】"
    },
    {
      href: "https://www.tiktok.com/",
      img: "https://obs.dianleida.net/public/popup/tiktok.png",
      text: "TIKTOK",
      functionName: "导航栏_主流平台【TIKTOK】"
    },
    {
      href: "https://www.temu.com/",
      img: "https://obs.dianleida.net/public/popup/temu.png",
      text: "TEMU",
      functionName: "导航栏_主流平台【TEMU】"
    }
  ],
  tab2: [
    {
      href: "https://shopee.com/",
      img: "https://obs.dianleida.net/public/popup/shopee.png",
      text: "Shopee",
      functionName: "导航栏_跨境平台【Shopee】"
    },
    {
      href: "https://www.shein.com/",
      img: "https://obs.dianleida.net/public/popup/shein.png",
      text: "SHEIN",
      functionName: "导航栏_跨境平台【SHEIN】"
    },
    {
      href: "https://www.ebay.com/",
      img: "https://obs.dianleida.net/public/popup/ebay.png",
      text: "eBay",
      functionName: "导航栏_跨境平台【eBay】"
    },
    {
      href: "https://www.wish.com/",
      img: "https://obs.dianleida.net/public/popup/wish.png",
      text: "Wish",
      functionName: "导航栏_跨境平台【Wish】"
    },
    {
      href: "https://www.lazada.com/",
      img: "https://obs.dianleida.net/public/popup/lazada.png",
      text: "Lazada",
      functionName: "导航栏_跨境平台【Lazada】"
    },
    {
      href: "https://www.mercadolibre.com/",
      img: "https://obs.dianleida.net/public/popup/meikeduo.png",
      text: "美客多",
      functionName: "导航栏_跨境平台【美客多】"
    },
    {
      href: "https://www.ozon.ru/",
      img: "https://obs.dianleida.net/public/popup/ozon.png",
      text: "Ozon",
      functionName: "导航栏_跨境平台【Ozon】"
    },
    {
      href: "http://global.gmarket.co.kr/",
      img: "https://obs.dianleida.net/public/popup/gmarket.png",
      text: "Gmarket",
      functionName: "导航栏_跨境平台【Gmarket】"
    }
  ],
  tab3: [
    {
      href: "https://www.taobao.com/",
      img: "https://obs.dianleida.net/public/popup/taobao.png",
      text: "淘宝",
      functionName: "导航栏_国内平台【淘宝】"
    },
    {
      href: "https://mobile.yangkeduo.com/",
      img: "https://obs.dianleida.net/public/popup/pinduoduo.png",
      text: "拼多多",
      functionName: "导航栏_国内平台【拼多多】"
    },
    {
      href: "https://www.douyin.com/",
      img: "https://obs.dianleida.net/public/popup/tiktok.png",
      text: "抖音",
      functionName: "导航栏_国内平台【抖音】"
    },
    {
      href: "https://www.kuaishou.com/",
      img: "https://obs.dianleida.net/public/popup/kuaishou.png",
      text: "快手",
      functionName: "导航栏_国内平台【快手】"
    },
    {
      href: "https://www.xiaohongshu.com/",
      img: "https://obs.dianleida.net/public/popup/xiaohongshu.png",
      text: "小红书",
      functionName: "导航栏_国内平台【小红书】"
    },
    {
      href: "https://www.jd.com/",
      img: "https://obs.dianleida.net/public/popup/jd.png",
      text: "京东",
      functionName: "导航栏_国内平台【京东】"
    }
  ]
};
let html = ``;

function tabHtml(tab) {
  localStorage.setItem("tab", tab);
  html = "";
  list[tab].forEach((it) => {
    html += ` <li>
            <a href="${it.href}" target="_blank" data-functionName="${it.functionName}" data-source="店雷达右上角导航栏">
                <img class="w40 h40" src="${it.img}"/>
                <p class="text">${it.text}</p>
            </a>
        </li>`;
  });
  document.querySelector(".ul-link").innerHTML = html;
}

function FnTab1() {
  let dom = document.getElementById("tab-1");
  dom.className = "tab-active";
  document.getElementById("tab-2").className = "";
  document.getElementById("tab-3").className = "";
  tabHtml("tab1");
}

function FnTab2() {
  let dom = document.getElementById("tab-2");
  dom.className = "tab-active";
  document.getElementById("tab-1").className = "";
  document.getElementById("tab-3").className = "";
  tabHtml("tab2");
}

function FnTab3() {
  let dom = document.getElementById("tab-3");
  dom.className = "tab-active";
  document.getElementById("tab-1").className = "";
  document.getElementById("tab-2").className = "";
  tabHtml("tab3");
}

(function() {
  let tab = localStorage.getItem("tab");
  if (tab === "1688") {
    FnTab1();
  } else if (tab === "pdd") {
    FnTab2();
  } else if (tab === "alibaba") {
    FnTab3();
  }
})();

document.querySelector(".wx").onmouseenter = function(e) {
  document.querySelector(".wx img").src = "https://obs.dianleida.net/public/popup/wx-hover.png?v=1";
};

document.querySelector(".wx").onmouseleave = function(e) {
  document.querySelector(".wx img").src = "https://obs.dianleida.net/public/popup/wx.png?v=1";
};

document.querySelector(".user").onmouseenter = function(e) {
  document.querySelector(".user img").src = "https://obs.dianleida.net/public/popup/qun-hover.png?v=1";
};

document.querySelector(".user").onmouseleave = function(e) {
  document.querySelector(".user img").src = "https://obs.dianleida.net/public/popup/qun.png?v=1";
};

document.getElementById("tab-1").onclick = function(e) {
  let target = e.target;
  md(target.getAttribute("data-functionName"), target.getAttribute("data-source"));
  FnTab1();
};

document.getElementById("tab-2").onclick = function(e) {
  let target = e.target;
  md(target.getAttribute("data-functionName"), target.getAttribute("data-source"));
  FnTab2();
};

document.getElementById("tab-3").onclick = function(e) {
  let target = e.target;
  md(target.getAttribute("data-functionName"), target.getAttribute("data-source"));
  FnTab3();
};

// document.querySelector('.website').onclick = function (e) {
//     let target = e.target
//     md(target.getAttribute('data-functionName'), target.getAttribute('data-source'))
// }

document.querySelector(".ul-link").onclick = function(e) {
  let target = e.target;
  let parentElement = target.parentElement;
  md(parentElement.getAttribute("data-functionName"), parentElement.getAttribute("data-source"));
};

chrome.tabs.query(
  {
    active: true,
    currentWindow: true
  },
  (tabs) => {
    tabArr = tabs;
    chrome.tabs.sendMessage(tabs[0].id, "popupGetInfo", (res) => {
      isDev = res?.isDev;
      isTest = res?.isTest;
      base = isTest ? "http://192.168.1.14/dld/" : "https://api.dianleida.net/dld/";
      admin = isTest ? "http://192.168.1.14/" : "https://www.dianleida.net/";
      getuserInfo();
    });
  }
);
