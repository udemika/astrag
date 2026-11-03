/* AstraG Control Panel: Branding, Settings -> Users, Full Login & IPTV Extensions */
;(function () {
    'use strict'
    if (!window.$ || !window.app) return

    // Rebrand Control Panel title to AstraG Control Panel
    document.title = 'AstraG Control Panel'
    app.setTitle = function (value) {
        var b = 'AstraG Control Panel'
        document.title = !!value ? value + ' - ' + b : b
    }

    // Clean up version footer text ("Astra AstraG 2.0" -> "AstraG 2.0")
    var fixVersionBadge = function () {
        var badges = document.querySelectorAll('.version.monospace')
        for (var i = 0; i < badges.length; i++) {
            var txt = badges[i].textContent || ''
            if (txt.indexOf('Astra AstraG') === 0) {
                badges[i].textContent = txt.replace(/^Astra\s+/, '')
            }
        }
    }
    if (window.MutationObserver) {
        var obs = new MutationObserver(fixVersionBadge)
        obs.observe(document.body, { childList: true, subtree: true })
    }

    // Restore session auth token into cookie before DOMContentLoaded $(function(){...}) runs
    try {
        var sessionToken = sessionStorage.getItem('astra_auth')
        if (sessionToken && !$.cookie.get('auth')) {
            $.cookie.set('auth', sessionToken)
        }
    } catch (e) {}

    // Capture Sign In form submission before authFormInit clears the password input
    document.addEventListener(
        'submit',
        function (event) {
            var form = event.target
            if (form && form.classList && form.classList.contains('auth')) {
                var inputs = form.querySelectorAll('input')
                var loginVal = ''
                var passVal = ''
                for (var i = 0; i < inputs.length; i++) {
                    var inp = inputs[i]
                    if (inp.type === 'password') {
                        passVal = inp.value
                    } else if (inp.type === 'text') {
                        loginVal = inp.value
                    }
                }
                if (loginVal && passVal) {
                    var token = loginVal + ':' + passVal
                    $.cookie.set('auth', token)
                    try {
                        sessionStorage.setItem('astra_auth', token)
                    } catch (e) {}
                }
            }
        },
        true,
    )

    // Ensure Settings -> Logout also clears sessionStorage
    if (window.SettingsModule) {
        var origSettingsClick = SettingsModule.click
        SettingsModule.click = function () {
            origSettingsClick.apply(this, arguments)
            var links = document.querySelectorAll('.modal.main-menu a')
            for (var i = 0; i < links.length; i++) {
                if (links[i].textContent === 'Logout') {
                    links[i].addEventListener('click', function () {
                        $.cookie.set('auth', undefined)
                        try {
                            sessionStorage.removeItem('astra_auth')
                        } catch (e) {}
                    })
                }
            }
        }
    }

    if (!window.SettingsUsersModule || !window.Form) return

    var value2date = function (v) {
        if (!v) return ''
        var d = new Date(v * 1e3)
        var dd = ('0' + d.getDate()).slice(-2)
        var dm = ('0' + (d.getMonth() + 1)).slice(-2)
        var dy = d.getFullYear()
        return dy + '-' + dm + '-' + dd
    }
    var daysInMonth = function (date) {
        return new Date(date.getYear(), date.getMonth() + 1, 0).getDate()
    }
    var daysInYear = function (date) {
        date = date.getYear() + 1900
        return (date % 4 == 0 && date % 100 != 0) ||
            (date % 4 == 0 && date % 100 == 0 && date % 400 == 0)
            ? 366
            : 365
    }
    var addDays = function (date, v) {
        var x = new Date(date)
        switch (v) {
            case '1d':
                v = 1
                break
            case '1w':
                v = 7
                break
            case '1m': {
                v = daysInMonth(date)
                var next = new Date(date.getYear(), date.getMonth() + 1, 1)
                next = daysInMonth(next)
                if (date.getDate() > next) v = v - date.getDate() + next
                break
            }
            case '1y':
                v = daysInYear(date)
                break
            default:
                v = 0
        }
        x.setDate(date.getDate() + v)
        return x
    }

    SettingsUsersModule.run = function () {
        app.on('set-user', function (event) {
            var hostId = event.host,
                data = event.data,
                appHost = app.hosts[hostId]
            if (!appHost || !data) return
            if (data.old_id && data.old_id !== data.id) {
                delete appHost.config.users[data.old_id]
                if (SettingsUsersModule.removeUser) {
                    SettingsUsersModule.removeUser(data.old_id)
                }
            }
            if (data.id == app.login || (data.old_id && data.old_id == app.login)) {
                location.reload(false)
                return
            }
            if (data.user && data.user.remove) {
                $.msg({ title: 'User "{0}" removed'.format(data.id) })
                delete appHost.config.users[data.id]
                if (SettingsUsersModule.removeUser) {
                    SettingsUsersModule.removeUser(data.id)
                }
            } else if (data.user) {
                appHost.config.users[data.id] = data.user
                $.msg({ title: 'User "{0}" saved'.format(data.id) })
                if (SettingsUsersModule.addUser) {
                    SettingsUsersModule.removeUser(data.id)
                    SettingsUsersModule.addUser(data.id, data.user)
                }
            }
        })
    }

    SettingsUsersModule.renderConfig = function () {
        var modal = this,
            form = new Form(modal.scope, modal),
            origLogin = modal.$origLogin || '',
            appHost = app.hosts[location.host]
        if (app.login != origLogin) {
            form.checkbox('Enable', 'user.enable').addAttr('data-false', 'false')
        }
        var inputLogin = form.input('Login', 'login')
        inputLogin.setRequired().addValidator(function (value) {
            if (!value) return false
            if (origLogin && value == origLogin) return true
            return appHost.config.users[value] == undefined
        })
        var passPlaceholder = origLogin ? 'New password (leave empty to keep current)' : 'Password'
        var inputPass = form.input('Password', 'user.password', passPlaceholder).addAttr('type', 'password')
        if (origLogin) {
            modal.scope.set('user.firstrun')
        } else {
            inputPass.setRequired()
        }
        form.input('Comment', 'user.comment')
        var typeMap = []
        $.forEach(SettingsUsersModule.typeMap, function (label, value) {
            typeMap.push({ value: value, label: label })
        })
        typeMap.sort(function (a, b) {
            return a.label.localeCompare(b.label)
        })
        if (app.login != origLogin) {
            form.choice('Type', 'user.type', typeMap)
        }
        var auth_enable = appHost.config.auth && !!appHost.config.auth.enable
        if (auth_enable) {
            form.input('Token', 'user.token')
            form.input('IP', 'user.ip')
            var d = form.scope.get('user.expire')
            if (d) form.scope.set('$user-expire', value2date(d))
            form.input('Expiration', '$user-expire', 'Format: YYYY-MM-DD')
                .on('input', function () {
                    var x = this.value || '0'
                    x = x.split('-')
                    if (x.length != 3) {
                        x = undefined
                    } else {
                        x = new Date(Number(x[0]), Number(x[1]) - 1, Number(x[2]))
                        x = x.getTime() / 1e3
                    }
                    form.scope.set('user.expire', x)
                })
                .addButton(
                    $.element('select')
                        .addClass('button icon icon-more')
                        .on('change', function () {
                            var x = form.scope.get('user.expire')
                            x = !!x ? new Date(x * 1e3) : new Date()
                            x = addDays(x, this.value)
                            x = x.getTime() / 1e3
                            form.scope.set('user.expire', x)
                            form.scope.set('$user-expire', value2date(x))
                            this.value = ''
                        })
                        .addChild($.element('option'))
                        .addChild($.element('option').setValue('1d').setText('+1 Day'))
                        .addChild($.element('option').setValue('1w').setText('+1 Week'))
                        .addChild($.element('option').setValue('1m').setText('+1 Month'))
                        .addChild($.element('option').setValue('1y').setText('+1 Year')),
                )
            form.input('Limit connections', 'user.conlimit', 'Default: 0 - unlimited')
            var categoriesMap = [{ value: '' }]
            $.forEach(app.hosts[location.host].config.categories, function (x) {
                categoriesMap.push({ value: x.name })
            })
            categoriesMap.sort(function (a, b) {
                return a.value.localeCompare(b.value)
            })
            var packages = form.scope.get('user.packages')
            form.header('Packages', 'New Package', function () {
                if (!packages) {
                    packages = []
                    form.scope.set('user.packages', packages)
                }
                packages.push('')
                form.scope.reset()
            })
            $.forEach(packages, function (v, k) {
                form.choice('', 'user.packages.' + k, categoriesMap).on('change', function () {
                    if (!this.value) {
                        packages.splice(k, 1)
                        if (!packages.length) form.scope.set('user.packages')
                        form.scope.reset()
                    }
                })
            })
        }
        $.forEach(SettingsUsersModule.modules, function (module) {
            form.hr()
            module.renderSettings.call(modal, form)
        })
        form.hr()
        var btnApply = $.element
            .button('Apply')
            .addClass('submit')
            .on('click', function () {
                if (modal.scope.validate()) modal.submit()
            })
        var btnCancel = $.element.button('Cancel').on('click', function () {
            modal.remove()
        })
        if (origLogin && app.login != origLogin)
            form.checkbox('Remove user', 'user.remove')
                .setDanger()
                .on('change', function () {
                    if (this.checked) btnApply.removeClass('submit').addClass('danger')
                    else btnApply.removeClass('danger').addClass('submit')
                })
        form.submit().addChild(btnApply, btnCancel)
    }

    SettingsUsersModule.openConfig = function (login) {
        var modalData = {}
        var origLogin = login || ''
        if (login == undefined) {
            modalData.login = ''
            modalData.user = { enable: true, type: 1 }
        } else {
            modalData.login = login
            modalData.user = $.clone(app.hosts[location.host].config.users[login]) || { enable: true, type: 1 }
            delete modalData.user.password
        }
        var modal = $.modal()
        modal.$origLogin = origLogin
        modal
            .bindScope(SettingsUsersModule.renderConfig, modalData)
            .on('submit', function () {
                var data = this.scope.serialize()
                if (data.user.remove) data.user = { remove: true }
                var appHost = app.hosts[location.host]
                var req = { cmd: 'set-user', id: data.login, user: data.user }
                if (origLogin && origLogin !== data.login) {
                    req.old_id = origLogin
                }
                if (origLogin && origLogin == app.login) {
                    var savedToken = $.cookie.get('auth')
                    if (!savedToken) {
                        try {
                            savedToken = sessionStorage.getItem('astra_auth')
                        } catch (e) {}
                    }
                    var oldPass = savedToken ? savedToken.split(':').slice(1).join(':') : 'admin'
                    var newPass = data.user.password || oldPass
                    var updated = data.login + ':' + newPass
                    $.cookie.set('auth', updated)
                    try {
                        sessionStorage.setItem('astra_auth', updated)
                    } catch (e) {}
                    app.login = data.login
                }
                appHost.request(
                    req,
                    function () {
                        if (origLogin && origLogin == app.login) {
                            location.reload(false)
                        }
                    },
                    function () {
                        $.err({ title: 'Failed to save user' })
                    },
                )
            })
    }
})()
