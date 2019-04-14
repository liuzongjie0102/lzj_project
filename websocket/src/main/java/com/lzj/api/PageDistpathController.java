package com.lzj.api;

import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.servlet.ModelAndView;

@Controller
@RequestMapping("/dist")
public class PageDistpathController {

    @RequestMapping("/index")
    public ModelAndView goIndex(){

        System.out.println("/index");
        ModelAndView mv = new ModelAndView();
        mv.setViewName("/dist/index.jsp");
        return mv;
    }
}
