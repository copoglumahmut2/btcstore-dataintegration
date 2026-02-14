package com.btc_store.dataintegration.controller.interceptor;

import com.btc_store.service.SiteService;
import com.btc_store.service.user.UserService;
import lombok.RequiredArgsConstructor;
import org.apache.commons.lang3.StringUtils;
import org.springframework.http.HttpHeaders;
import org.springframework.stereotype.Component;
import org.springframework.web.servlet.HandlerInterceptor;
import util.Messages;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.util.Locale;

import static org.springframework.http.HttpHeaders.AUTHORIZATION;

@Component
@RequiredArgsConstructor
public class ValidationLocaleInterceptor implements HandlerInterceptor {

    protected final SiteService siteService;
    protected final UserService userService;

    @Override
    public boolean preHandle(HttpServletRequest request, HttpServletResponse response, Object handler) {
        var isoCode = request.getParameter("isoCode");
        if (StringUtils.isNotEmpty(isoCode) && !StringUtils.equalsIgnoreCase(isoCode, "null")) {
            Messages.setMessagesLocale(Locale.forLanguageTag(isoCode));
        } else if (StringUtils.startsWith(request.getHeader(AUTHORIZATION), "Bearer ")) {
            try {
                var userModel = userService.getCurrentUser();
                Messages.setMessagesLocale(Locale.forLanguageTag(userModel.getLanguage().getCode()));
            } catch (Exception e) {
                setCurrentSiteLanguage(request);
            }
        } else {
            setCurrentSiteLanguage(request);
        }
        return true;
    }

    protected void setCurrentSiteLanguage(HttpServletRequest request) {
        var host = request.getHeader(HttpHeaders.HOST);
        var siteModel = siteService.getSiteModelByDomain(StringUtils.split(host, ":")[0]);
        Messages.setMessagesLocale(Locale.forLanguageTag(siteModel.getLanguage().getCode()));
    }
}
