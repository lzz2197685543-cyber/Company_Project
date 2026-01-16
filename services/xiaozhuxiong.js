const CryptoJS = require("crypto-js");


function XB(e, t, n) {
    let r, o, s = e;
    typeof s == "object" && (s.systemDate,
        delete s.systemDate,
        s = JSON.stringify(s)),
        r = CryptoJS.enc.Utf8.parse(t),
        o = CryptoJS.enc.Utf8.parse(n);
    const i = CryptoJS.enc.Utf8.parse(s);
    return CryptoJS.AES.encrypt(i, r, {
        iv: o,
        mode: CryptoJS.mode.CBC,
        padding: CryptoJS.pad.Pkcs7
    }).ciphertext.toString()
}

function eve(sys) {
    let e = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
    // const t = Zme.get("systemCode");
    const t = sys

    if (!t)
        return "";
    const n = Math.floor(Math.random() * 10)
        , r = [2, 3, 4, 5, 6][Math.floor(Math.random() * 5)]
        , o = Math.floor(Math.random() * 10)
        , s = t.slice(n, n + r);
    for (let h = 0; h < s.length; h++)
        e = e.replace(s[h], "");
    const i = [];
    for (; i.length < 15;) {
        const h = Math.floor(Math.random() * e.length)
            , p = e[h];
        e = e.replace(p, ""),
            i.push(p)
    }
    const a = i.join("")
        , l = a.slice(o, o + s.length)
        , u = `${a.replace(l, s)}${n}${r}${o}`
        , c = "ZL3O46FH5FUK33HX3ILIRQC891YOYIWM"
        , d = `${u}${XB(u.slice(13), c, c.slice(-16))}`;
    return XB(d, c, c.slice(-16))
}

function get_utoken(sys,token) {
    r = eve(sys)
    o = token
    s = o + "." + r
    return s
}


// console.log(get_utoken())